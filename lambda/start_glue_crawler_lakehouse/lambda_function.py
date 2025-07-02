import boto3

def lambda_handler(event, context):
    crawler_name = "ecommerce-delta-crawler"
    glue = boto3.client("glue")

    # Get crawler state
    try:
        crawler_info = glue.get_crawler(Name=crawler_name)
        state = crawler_info["Crawler"]["State"]

        if state == "READY":
            glue.start_crawler(Name=crawler_name)
            return {"message": f"Started crawler: {crawler_name}"}
        else:
            return {"message": f"Crawler {crawler_name} already running. Current state: {state}"}

    except Exception as e:
        return {
            "errorMessage": str(e),
            "errorType": type(e).__name__
        }
