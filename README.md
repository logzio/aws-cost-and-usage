## Overview
 
This is an AWS Lambda function that collects AWS cost and usage csv report and sends them to Logz.io in bulk, over HTTP.
For more snapshot and explanations you can visit our blog here.

## Prerequisite
Before you follow the steps bellow please make sure you enabled AWS to generate a report to your target bucket. If you 
are not sure how to do it, you can check this post(TODO - add link).

## Step 1 - Create a new lambda 
1. Sign in to your AWS account and open the AWS Lambda console.
2. Click **Create function**, to create a new Lambda function.
3. Select Author from scratch, and enter the following information:
  - Name -  Enter a name for your new Lambda function. We suggest adding the log type to the name.
  - Runtime - From the drop-down menu, select Python 2.7 as the function’s runtime.
  - Role - Make sure to add the following policy to your Lambda:
    ```   
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:Get*",
                    "s3:List*"
                ],
                "Resource": "*"
            }
        ]
    }   
    ```
    Hit the **Create Function** button in the bottom-right corner of the page.
    
## Step 2 - Uploading and configuring the Logz.io Lambda shipper
1. Download 'lambda_function.py' and 'shipper.py' from *src* folder and zip them using the following: 
 `zip logzio-cost-and-usage-shipper lambda_function.py shipper.py`
2. In the Function Code section, open the Code entry type menu, and select *Upload a .ZIP file*.
3. Select the zip you created.
4. Set the following:
  - TOKEN: Your Logz.io account token. Can be retrieved on the Settings page in the Logz.io UI.
  - URL: The Logz.io listener URL. If you are in the EU region insert https://listener-eu.logz.io:8071. Otherwise, use https://listener.logz.io:8071. You can tell which region you are in by checking your login URL - *app.logz.io* means you are in the US. *app-eu.logz.io* means you are in the EU.
  - REPORT_NAME: *See below picture where can you take it from*
  - REPORT_PATH: *See below picture where can you take it from*
  - S3_BUCKET_NAME: *See below picture where can you take it from* 
5. In the Basic Settings section, we recommend to start by setting memory to 1024(MB) and a 5(MIN) timeout, and then subsequently adjusting these values based on trial and error, and according to your Lambda usage.
6. Leave the other settings as default
![Alt text](report_fields.jpg?raw=true)

## Step 3 - Setting scheduling trigger
1. Under Add triggers at the top of the page, select the CloudWatch event trigger.
2. In the Configure triggers section, select 'Create a new rule' and enter the 'Rule Name' and 'Rule Description'. 
3. Under 'Rule type' select 'Schedule expression', and in the 'Schedule expression' tab enter your report shipping rate.
Notice that AWS publish report updates up to three times a day so there is no need to set the rate too frequently.
We recommend to start with 10 hour rate: `rate(10 hours)`
4. Click **Add** to add the trigger and **Save** at the top of the page to save all your configurations.

[here]: https://support.logz.io/hc/en-us/articles/210205985-Which-log-types-are-preconfigured-on-the-Logz-io-platform-


    
    
    
    
    
    
    
    
    
    
    