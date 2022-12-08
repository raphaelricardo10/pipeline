python3 -m pipeline \
    --region $REGION \
    --runner DataflowRunner \
    --project $PROJECT_ID \
    --temp_location $GS_TEMP \
    --template_location $GS_TEMPLATES/multisensor_pipeline \
    --requirements_file requirements.txt \
    --dry-run
    