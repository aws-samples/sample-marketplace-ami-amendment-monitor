import json
import boto3
import os
import time
import re
import logging
import hashlib
import traceback
from datetime import datetime
from botocore.exceptions import ClientError, BotoCoreError, EndpointResolutionError

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))

# Initialize AWS clients
ec2 = boto3.client('ec2')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['TABLE'])

def should_send_notification(instance_id, before_type, after_type, window_minutes=5):
    """
    Prevent duplicate or flood notifications.
    Returns True if notification should be sent, False if suppressed.
    Uses DynamoDB to track recent notifications with a short TTL.
    """
    change_hash = hashlib.sha256(
        f"{instance_id}:{before_type}:{after_type}".encode()
    ).hexdigest()
    dedup_key = f"notification#{instance_id}#{change_hash}"

    try:
        response = table.get_item(Key={'instanceId': dedup_key})
        if response.get('Item'):
            print(f"⊘ Duplicate notification suppressed for {instance_id}")
            return False

        # Record this notification with short TTL
        table.put_item(Item={
            'instanceId': dedup_key,
            'timestamp': datetime.utcnow().isoformat(),
            'ttl': int(time.time()) + (window_minutes * 60)
        })
        return True
    except Exception:
        # Fail open — send notification if dedup check fails
        logger.warning("Deduplication check failed, sending notification anyway")
        return True

def sanitize_email_field(text, max_length=200):
    """Remove control characters and limit length to prevent email injection."""
    if not text:
        return 'unknown'
    
    # Strip newlines, carriage returns, and ASCII control characters
    sanitized = re.sub(r'[\r\x00-\x1f\x7f]', '', str(text))
    return sanitized[:max_length]

def validate_instance_id(instance_id):
    """Validate EC2 instance ID format: must match i-[8 to 17 hex chars]."""
    if not instance_id:
        raise ValueError("Instance ID is required")
    if not isinstance(instance_id, str):
        raise ValueError(f"Instance ID must be a string, got {type(instance_id)}")
    if not re.match(r'^i-[0-9a-f]{8,17}$', instance_id):
        raise ValueError(f"Invalid instance ID format: {instance_id}")
    return instance_id

def validate_instance_type(instance_type):
    """Validate EC2 instance type format: must match family.size (e.g., m5.large)."""
    if not instance_type:
        raise ValueError("Instance type is required")
    if not isinstance(instance_type, str):
        raise ValueError(f"Instance type must be a string, got {type(instance_type)}")
    if not re.match(r'^[a-z][0-9][a-z0-9]*\.[a-z0-9]+$', instance_type.lower()):
        raise ValueError(f"Invalid instance type format: {instance_type}")
    return instance_type

def send_email_notification(item):
    """
    Send email notification about instance type change
    Only sends if email notifications are enabled
    """
    # Check if email notifications are enabled
    if os.environ.get('ENABLE_EMAIL_NOTIFICATIONS', 'false').lower() != 'true':
        print("Email notifications disabled, skipping")
        return
    
    email_from = os.environ.get('EMAIL_FROM', '')
    email_recipients = [e.strip() for e in os.environ.get('EMAIL_RECIPIENT', '').split(',') if e.strip()]
    
    if not email_from or not email_recipients:
        print("Email addresses not configured, skipping notification")
        return
    
    try:
        ses = boto3.client('ses', region_name='us-east-1')
        
        # Get email templates from environment
        subject_template = os.environ.get('EMAIL_SUBJECT_TEMPLATE', 'Instance Type Change: {instance_id}')
        body_template = os.environ.get('EMAIL_BODY_TEMPLATE', 'Instance {instance_id} changed from {before_type} to {after_type}')
        
        # Format email content
        subject = subject_template.format(
            instance_id=sanitize_email_field(item.get('instanceId', 'unknown'))
        )
        
        body = body_template.format(
            instance_id=sanitize_email_field(item.get('instanceId', 'unknown')),
            product_name=sanitize_email_field(item.get('productName', 'Unknown')),
            product_id=sanitize_email_field(item.get('productId', 'Unknown')),
            offer_id=sanitize_email_field(item.get('offerId', 'Unknown')),
            agreement_id=sanitize_email_field(item.get('agreementId', 'Unknown')),
            before_type=sanitize_email_field(item.get('beforeInstanceType', 'unknown')),
            after_type=sanitize_email_field(item.get('afterInstanceType', 'unknown')),
            changed_at=sanitize_email_field(item.get('changedAt', 'unknown')),
            validated=sanitize_email_field(item.get('instanceTypeValidated', 'N/A')),
            skipped=sanitize_email_field(item.get('validationSkipped', 'N/A'))
        )

        # Send email
        response = ses.send_email(
            Source=email_from,
            Destination={'ToAddresses': email_recipients},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
            
        print(f"✅ Email sent successfully: {response['MessageId']}")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'MessageRejected':
            logger.warning(f"Email rejected by SES: {e}")
            print("Email rejected - check sender/recipient verification")
        elif error_code in ('InvalidParameterValue', 'InvalidParameterCombination'):
            logger.error(f"Invalid email parameters: {e}")
            print("Invalid email configuration - check template variables")
        elif error_code == 'ConfigurationSetDoesNotExist':
            logger.error(f"SES configuration error: {e}")
        else:
            logger.error(f"SES ClientError: {e}")
    except BotoCoreError as e:
        logger.error(f"BotoCore error sending email: {e}")
        print("Network/SDK error sending email")
    except Exception as e:
        logger.error(f"Unexpected error: {traceback.format_exc()}")
        print("An unexpected error occurred sending email notification — see CloudWatch logs for details")

def is_excluded_by_tag(instance):
    """
    Check if instance should be excluded from monitoring.
    Looks for tag: aws-marketplace-monitor=false
    """
    tags = instance.get('Tags', [])
    for tag in tags:
        if tag['Key'] == 'aws-marketplace-monitor' and tag['Value'] == 'false':
            print(f"Instance excluded by tag aws-marketplace-monitor=false")
            return True
    return False


def get_marketplace_agreement(product_code):
    """
    Get marketplace agreement details including:
    - Agreement ID and name
    - Product name (friendly name from catalog)
    - Product ID (from agreement resources)
    - Offer ID (product code)
    - Allowed instance types from agreement terms
    """
    marketplace = boto3.client('marketplace-agreement', region_name='us-east-1')
    
    try:
        # Search for active marketplace agreement
        agreements = marketplace.search_agreements(
            catalog='AWSMarketplace',
            filters=[
                {'name': 'PartyType', 'values': ['Acceptor']},
                {'name': 'AgreementType', 'values': ['PurchaseAgreement']},
                {'name': 'Status', 'values': ['ACTIVE']},
                {'name': 'OfferId', 'values': [product_code]}
            ]
        )
        
        if not agreements.get('agreementViewSummaries'):
            return None
        
        summary = agreements['agreementViewSummaries'][0]
        agreement_id = summary['agreementId']
        
        # Extract Product ID from resources (AMI Product ID)
        product_id = 'Unknown'
        resources = summary.get('proposalSummary', {}).get('resources', [])
        for resource in resources:
            if resource.get('type') == 'AmiProduct':
                product_id = resource.get('id', 'Unknown')
                break
        
        # Extract allowed instance types from agreement terms
        allowed_types = []
        try:
            terms = marketplace.get_agreement_terms(agreementId=agreement_id)
            for term in terms.get('acceptedTerms', []):
                if 'configurableUpfrontPricingTerm' in term:
                    config = term['configurableUpfrontPricingTerm'].get('configuration', {})
                    dimensions = config.get('dimensions', [])
                    for dim in dimensions:
                        dim_key = dim.get('dimensionKey')
                        if dim_key:
                            allowed_types.append(dim_key)
        
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                logger.warning(f"Agreement terms not found: {agreement_id}")
                print("Agreement terms not available")
            elif error_code in ('AccessDeniedException', 'UnauthorizedException'):
                logger.error(f"No permission to read agreement terms: {e}")
                print("IAM permissions missing for GetAgreementTerms")
            else:
                logger.error(f"Marketplace ClientError getting terms: {e}")
        except BotoCoreError as e:
            logger.error(f"BotoCore error getting agreement terms: {e}")
        except KeyError as e:
            logger.warning(f"Missing expected field in agreement terms: {e}")
            print("Agreement terms structure unexpected")
        except Exception as e:
            logger.error(f"Unexpected error: {traceback.format_exc()}")
            print("An unexpected error occurred getting agremment terms — see CloudWatch logs for details")
        
        # Get product name from original marketplace AMI
        product_name = 'Unknown'
        try:
            # Search for marketplace AMIs with this product code
            # Using Owners=['aws-marketplace'] ensures we get the original AMI, not user copies
            images = ec2.describe_images(
                Filters=[
                    {'Name': 'product-code', 'Values': [product_code]},
                    {'Name': 'state', 'Values': ['available']}
                ],
                Owners=['aws-marketplace']
            )
            
            if images.get('Images'):
                # Get the most recent AMI (sorted by creation date)
                ami = sorted(images['Images'], key=lambda x: x.get('CreationDate', ''), reverse=True)[0]
                product_name = ami.get('Description') or ami.get('Name') or 'Unknown'
                print(f"Got product name from marketplace AMI: {product_name}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidAMIID.NotFound':
                logger.warning(f"AMI not found for product code: {product_code}")
            elif error_code in ('UnauthorizedOperation', 'AccessDenied'):
                logger.error(f"No permission to describe images: {e}")
            else:
                logger.error(f"EC2 ClientError describing images: {e}")
        except BotoCoreError as e:
            logger.error(f"BotoCore error describing images: {e}")
        except (KeyError, IndexError) as e:
            logger.warning(f"Unexpected AMI response structure: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {traceback.format_exc()}")
            print("An unexpected error occurred get product name from marketplace AMI — see CloudWatch logs for details")
        
        # Fallback to Offer ID if product name not found
        if product_name == 'Unknown':
            product_name = summary.get('offerId', 'Unknown')
        
        return {
            'agreement_id': agreement_id,
            'agreement_name': summary.get('agreementType', 'Unknown'),
            'product_name': product_name,
            'product_id': product_id,
            'offer_id': product_code,
            'allowed_types': allowed_types
        }
    
    
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ValidationException':
            logger.error(f"Invalid search parameters: {e}")
            print("Invalid marketplace search parameters")
            return None
        elif error_code in ('AccessDeniedException', 'UnauthorizedException'):
            logger.error(f"No permission to search agreements: {e}")
            print("IAM permissions missing for SearchAgreements")
            return None
        else:
            logger.error(f"Marketplace ClientError: {e}")
            return None
    except BotoCoreError as e:
        logger.error(f"BotoCore error searching agreements: {e}")
        return None
    except (KeyError, IndexError) as e:
        logger.warning(f"Unexpected agreement response structure: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {traceback.format_exc()}")
        print("An unexpected error occurred getting marketplace agreement — see CloudWatch logs for details")
        return None

def is_marketplace_instance(instance_id):
    """
    Check if instance is from marketplace AMI and not excluded by tag.
    Returns: (is_marketplace, instance) - tuple because we need both the boolean AND instance data
    """
    try:
        # Get instance details
        response = ec2.describe_instances(InstanceIds=[instance_id])
        if not response['Reservations']:
            return False, None
            
        instance = response['Reservations'][0]['Instances'][0]
            
        # Check if instance is excluded by tag
        if is_excluded_by_tag(instance):
            print("Excluded by tag")
            return False, None
            
        # Check if instance has marketplace product codes
        product_codes = instance.get('ProductCodes', [])
        if not product_codes:
            return False, None
            
        if product_codes[0].get('ProductCodeType') != 'marketplace':
            return False, None

        print(f"✓ Marketplace instance: {instance_id}")
        return True, instance
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidInstanceID.NotFound':
            logger.info(f"Instance not found: {instance_id}")
            print(f"Instance {instance_id} not found, skipping")
            return False, None
        elif error_code == 'InvalidInstanceID.Malformed':
            logger.warning(f"Malformed instance ID: {instance_id}")
            return False, None
        elif error_code in ('UnauthorizedOperation', 'AccessDenied'):
            logger.error(f"No permission to describe instance: {instance_id}")
            print("IAM permissions missing for DescribeInstances")
            return False, None
        else:
            logger.error(f"EC2 ClientError: {e}")
            return False, None
    except BotoCoreError as e:
        logger.error(f"BotoCore error describing instance: {e}")
        return False, None
    except (KeyError, IndexError) as e:
        logger.error(f"Unexpected EC2 response structure: {e}")
        return False, None
    except Exception as e:
        logger.error(f"Unexpected error: {traceback.format_exc()}")
        print("An unexpected error occurred checking marketplace instance — see CloudWatch logs for details")
        return False, None


def validate_agreement(instance, type_to_validate):
    """
    Get marketplace agreement and validate instance type.
    Returns: (agreement, is_valid) - tuple because we need both agreement data AND validation result
    """
    try:
        # Check if agreement verification should be skipped
        skip_agreement = os.environ.get('SKIP_AGREEMENT_VERIFICATION', 'false').lower() == 'true'
        
        # Get marketplace agreement details
        product_codes = instance.get('ProductCodes', [])
        if not product_codes:
            return None, False
            
        agreement = get_marketplace_agreement(product_codes[0]['ProductCodeId'])
        if not agreement:
            return None, False
        
        print(f"✓ Agreement found: {agreement['agreement_id']}")
        
        # If agreement verification is skipped, always return valid
        if skip_agreement:
            print("⊘ Agreement verification skipped by configuration")
            return agreement, True
        
        # Validate the instance type against agreement's allowed types
        is_valid = agreement['allowed_types'] and type_to_validate in agreement['allowed_types']
        
        if is_valid:
            print(f"✓ Instance type '{type_to_validate}' is valid in agreement")
        else:
            print(f"✗ Instance type '{type_to_validate}' is NOT valid in agreement")
        
        return agreement, is_valid
        
    except ClientError as e:
        logger.error(f"AWS API error during validation: {e}")
        return None, False
    except BotoCoreError as e:
        logger.error(f"BotoCore error during validation: {e}")
        return None, False
    except (KeyError, IndexError, TypeError) as e:
        logger.warning(f"Data structure error during validation: {e}")
        print("Unexpected data format in agreement validation")
        return None, False
    except Exception as e:
        logger.error(f"Unexpected error: {traceback.format_exc()}")
        print("An unexpected error occurred validating agreement — see CloudWatch logs for details")
        return None, False


def handler(event, context):
    """Main Lambda handler"""
    print(json.dumps(event))
    
    detail = event.get('detail', {})
    detail_type = event.get('detail-type')
    
    # CASE 1: EC2 Instance State Change (stopping/stopped)
    # Logic: Only check if marketplace instance, always record if true
    if detail_type == 'EC2 Instance State-change Notification':
        try:
            instance_id = validate_instance_id(detail.get('instance-id'))
        except ValueError as e:
            print(f"Input validation failed: {e}")
            return {'statusCode': 400}

        state = detail.get('state')
        
        if not instance_id or state not in ['stopping', 'stopped']:
            return {'statusCode': 200}
        
        print(f"📍 State change: {instance_id} is {state}")
        
        try:
            # Only check if marketplace instance (no agreement verification)
            is_marketplace, instance = is_marketplace_instance(instance_id)
            if not is_marketplace:
                return {'statusCode': 200}
            
            current_type = instance['InstanceType']
            print(f"✅ Captured: {instance_id} = {current_type}")
            
            # Always record state for marketplace instances (no agreement data needed)
            table.put_item(Item={
                'instanceId': instance_id,
                'beforeInstanceType': current_type,
                'capturedAt': event.get('time', datetime.utcnow().isoformat() + 'Z'),
                'ttl': int(time.time()) + (90 * 86400)  # 90 days TTL
            })
            
            return {'statusCode': 200}
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"AWS API error in state change handler: {error_code} - {e}")
            print(f"AWS API error: {error_code}")
            return {'statusCode': 500, 'body': f'AWS API error: {error_code}'}
        except BotoCoreError as e:
            logger.error(f"BotoCore error in state change handler: {e}")
            return {'statusCode': 500, 'body': 'SDK error'}
        except KeyError as e:
            logger.error(f"Missing required field in event: {e}")
            return {'statusCode': 400, 'body': f'Invalid event structure: {e}'}
        except Exception as e:
            logger.error(f"Unexpected error: {traceback.format_exc()}")
            print("An unexpected error occurred handling state change — see CloudWatch logs for details")
            return {'statusCode': 500}
    
    # CASE 2: ModifyInstanceAttribute (CloudTrail event)
    # Logic: Check marketplace + validate agreement, always record, only email if valid
    elif detail_type == 'AWS API Call via CloudTrail':
        if detail.get('eventName') != 'ModifyInstanceAttribute':
            return {'statusCode': 200}
        
        request_params = detail.get('requestParameters', {})

        try:
            instance_id = validate_instance_id(request_params.get('instanceId'))
            instance_type_obj = request_params.get('instanceType', {})
            new_type = validate_instance_type(
                instance_type_obj.get('value') if isinstance(instance_type_obj, dict) else None
            )
        except ValueError as e:
            print(f"Input validation failed: {e}")
            return {'statusCode': 400}

        if not instance_id or not new_type:
            return {'statusCode': 200}
        
        print(f"🔄 Modify detected: {instance_id} → {new_type}")
        
        try:
            # Check if marketplace instance
            is_marketplace, instance = is_marketplace_instance(instance_id)
            if not is_marketplace:
                return {'statusCode': 200}
            
            # Get the before type and capturedAt from DynamoDB
            db_item = table.get_item(Key={'instanceId': instance_id})
            item_data = db_item.get('Item', {})
            
            # Determine the "before" type: use afterInstanceType if exists, otherwise beforeInstanceType
            before_type = item_data.get('afterInstanceType') or item_data.get('beforeInstanceType', 'unknown')
            
            # Preserve the original capturedAt timestamp
            captured_at = item_data.get('capturedAt', datetime.utcnow().isoformat() + 'Z')
            
            if before_type == 'unknown':
                print("⚠️ No previous state found in DynamoDB")
                return {'statusCode': 200}
            
            # Verify the change was successful
            actual_type = instance['InstanceType']
            if actual_type != new_type:
                print(f"⚠️ Type mismatch: expected {new_type}, got {actual_type}")
                return {'statusCode': 200}
            
            # Validate the OLD instance type against agreement
            agreement, is_valid = validate_agreement(instance, type_to_validate=before_type)
            
            # Skip if no actual change occurred
            if before_type == new_type:
                print(f"⊘ No change detected: {instance_id} remains {before_type}")
                return {'statusCode': 200}
            
            print(f"✅ Recording change: {instance_id}: {before_type} → {new_type}")
            
            # Always record the change with basic info (preserve capturedAt)
            item = {
                'instanceId': instance_id,
                'beforeInstanceType': before_type,
                'afterInstanceType': new_type,
                'capturedAt': captured_at,  # Preserve original capture time
                'changedAt': detail.get('eventTime', datetime.utcnow().isoformat() + 'Z'),
                'ttl': int(time.time()) + (90 * 86400)
            }

            should_send_email = should_send_notification(item.get('instanceId'),
                                                        item.get('beforeInstanceType', ''),
                                                        item.get('afterInstanceType', ''))

            _internal_email_block = False

            if not should_send_email:
                print(f"Not sending email due to internal validation")
                _internal_email_block = True
            
            # Add agreement details if available
            if agreement:
                item.update({
                    'agreementId': agreement['agreement_id'],
                    'agreementName': agreement['agreement_name'],
                    'productName': agreement['product_name'],
                    'productId': agreement['product_id'],
                    'offerId': agreement['offer_id'],
                    'isTriggeredTheAlert': bool(is_valid),  # Explicitly convert to boolean
                    'internalBlock': _internal_email_block
                })
            else:
                print("⚠️ No agreement found, recording without agreement details")
                item['isTriggeredTheAlert'] = False  # Explicit boolean False

            
            table.put_item(Item=item)
            
            # Send email ONLY if agreement exists AND validation passed
            if agreement and is_valid and not _internal_email_block:
                send_email_notification(item)
            else:
                if not agreement:
                    print(f"⊘ Email skipped - no agreement found")
                else:
                    print(f"⊘ Email skipped - '{before_type}' not valid in agreement")
            
            return {'statusCode': 200}
        
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"AWS API error in state handling modify handler: {error_code} - {e}")
            print(f"AWS API error: {error_code}")
            return {'statusCode': 500, 'body': f'AWS API error: {error_code}'}
        except BotoCoreError as e:
            logger.error(f"BotoCore error in state change handler: {e}")
            return {'statusCode': 500, 'body': 'SDK error'}
        except KeyError as e:
            logger.error(f"Missing required field in event: {e}")
            return {'statusCode': 400, 'body': f'Invalid event structure: {e}'}
        except Exception as e:
            logger.error(f"Unexpected error: {traceback.format_exc()}")
            print("An unexpected error occurred handling modify — see CloudWatch logs for details")
            return {'statusCode': 500}
    
    return {'statusCode': 200}
