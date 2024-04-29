from setup import creds
from reporting import product_reports

font = 'Lora'

boiler_plate = """ 
<!doctype html>
<html>
  <head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
    """

css = """
    <style>
        html {
            font-family: Lora;
        }

        h1 {  
            color:#000000;  
            font-size: 35px;  
            font-weight: 300;  
            margin-bottom: 0px;

        }

        h2 {  
            color:#000000;  
            font-weight: 200;  
            font-size: 20px;  
            margin-top: 20px;
            margin-bottom: 5px;
        }

        h3 {  
            color:#000000;  
            font-size: 16px;  
            font-weight: 300;  
            margin-bottom: 20px;
        }

        h4 {  
            color:#000000;  
            margin-top: 15px;  
            margin-bottom: 0px;  
            font-size: 16px;  
            font-weight: 300;
        }

        h5 {  
            font-family: 'Arial';  
            margin: 0px;
            font-size: 13px;
        }
        
            .preheader {  
                visibility: hidden;
        }
        
        p {
            margin: 5px 0px 15px 0px;
        }

        .rank {
            margin-top: 15px;
        }
        </style>
"""

body_start = """
  </head>
  <body style="background-color:#ffffff;">
    <span class="preheader">Administrative Report</span>
        <div>
            <div>
                <img class="logo" src="cid:image1">
            </div>

            <div>
                        """

body_end = f"""

            </div>
  </body>
</html>
"""


def create_email_html():
    contents = product_reports.report_generator()
    return boiler_plate + css + body_start + contents + body_end
