"""Non-secret settings used by the Streamlit dashboard.

Copy this file to ``config.py`` and replace the bucket name. AWS credentials
must come from an IAM role, ``~/.aws/credentials``, or environment variables.
Never add access keys to this file.
"""

BUCKET_NAME = "replace-with-your-unique-bucket-name"
AWS_REGION = "ap-south-1"
ATHENA_DATABASE = "amazon_appliances"
ATHENA_OUTPUT_LOCATION = f"s3://{BUCKET_NAME}/athena-query-results/"
ATHENA_WORKGROUP = "primary"

# Optional named profile from ~/.aws/credentials. Use an empty string when the
# dashboard runs with an IAM role or with the default AWS profile.
AWS_PROFILE = "amazon-appliances"
