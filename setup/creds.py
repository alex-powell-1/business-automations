from datetime import datetime
import json

# Reference Config File
config_file = "//mainserver/CPSQL.1/business_automations/config.json"

with open(config_file) as f:
    config_data = json.load(f)
    # SQL
    sql = config_data['keys']['main_server']
    SERVER = sql['address']
    DATABASE = sql['database']
    USERNAME = sql['db_username']
    PASSWORD = sql['db_password']
    # BigCommerce Integration
    bc_category_table = 'SN_CATEG'
    bc_category_item_table = 'SN_CATEG_ITEM'
    bc_product_table = 'SN_PROD'
    bc_image_table = 'SN_IMAGES'
    bc_customer_table = 'SN_CUST'
    bc_order_table = 'SN_ORDERS'
    bc_order_detail_table = 'SN_ORD_DETAIL'
    bc_order_status_table = 'SN_ORD_STATUS'


    # ngrok
    ngrok = config_data['keys']['flask_server']
    ngrok_domain = ngrok['ngrok_domain']
    flask_server_name = ngrok['flask_server_name']

    # NCR Counterpoint API
    cp_api = config_data['keys']['counterpoint_api']
    cp_api_user = cp_api['cp_api_user']
    cp_api_key = cp_api['cp_api_key']
    cp_api_server = cp_api['cp_api_server']
    cp_api_order_server = cp_api['cp_api_order_server']

    # Twilio
    twilio = config_data['keys']['twilio']
    twilio_phone_number = twilio['twilio_phone_number']
    twilio_account_sid = twilio['twilio_account_sid']
    twilio_auth_token = twilio['twilio_auth_token']

    # BigCommerce
    big_commerce = config_data['keys']['big_commerce']
    big_client_id = big_commerce['client_id']
    big_access_token = big_commerce['access_token']
    big_store_hash = big_commerce['store_hash']

    # webDav
    web_dav = config_data['keys']['web_dav']
    web_dav_server = web_dav['server']
    web_dav_product_photos = web_dav['web_dav_product_photos']
    public_web_dav_photos = web_dav['public_web_dav_photos']
    web_dav_user = web_dav['user']
    web_dav_pw = web_dav['password']

    # Mailerlite
    mailerlite = config_data['keys']['mailerlite']
    mailerlite_token = mailerlite['token']
    wholesale_mailing_list = mailerlite['wholesale_mailing_list']
    retail_all_mailing_list = mailerlite['retail_all_mailing_list']
    house_plant_buyers = mailerlite['house_plant_buyers']
    workshop_attendees = mailerlite['workshop_attendees']

    # Company
    company = config_data['company']
    company_name = company['name']
    company_phone = company['phone']
    company_product_brand = company['product_brand']
    review_link = company['review_link']
    company_address_html = company['address_html']
    company_hours = company['hours']

    # Item Images Folder
    photo_path = company['images']

    # Network Notification
    network_notification_phone = company['network_notification_phone']

    # GMail Account
    sales_email = config_data['keys']['gmail']['sales']['username']
    sales_password = config_data['keys']['gmail']['sales']['password']

    # Reports
    administrative_report = config_data['reports']['administrative_report']
    item_report = config_data['reports']['item_report']
    lead_email = config_data['reports']['marketing_leads']

    # Staff
    staff = config_data['staff']

    # Counterpoint
    on_sale_category = config_data['counterpoint']['categories']['sale']

    # Logging
    log_path = config_data['logs']['general']['root']
    business_automation_log = f"{log_path}/business_automations/automations_{datetime.now():%m_%d_%y}.csv"
    new_customer_log = f"{log_path}/new_customers.csv"
    design_lead_log = f"{log_path}/design_request_leads.csv"
    buffer_log = f"{log_path}/buffer_log/buffer_log.csv"
    product_photo_log = f"{log_path}/logs/product_photos.csv"
    wholesale_pricing_tier_log = f"{log_path}/wholesale_price_tiers/wholesale_price_tiers_{datetime.now():%m_%d_%y}.csv"

# Twilio Test Recipients
test_customer = ['105786']
test_customers = [{"PHONE_1": "+18282341265", "FST_NAM": "Alex", "LOY_PTS_BAL": 5},
                  {"PHONE_1": "+18282341265", "FST_NAM": "John", "LOY_PTS_BAL": 1},
                  {"PHONE_1": "+18282341265", "FST_NAM": "Mary", "LOY_PTS_BAL": 85}]

# test_landline = [{"PHONE_1": "+18284372425", "FST_NAM": "Kathy", "LOY_PTS_BAL": 5}]


email_footer = "Settlemyre Nursery, 1387 Drexel Road, Valdese, NC 28690"

logo = "./setup/images/Logo.jpg"
web_logo = "https://settlemyrenursery.com/content/Settlemyre_Logo_Large.png"


# Log Location
inactive_product_log = f"//mainserver/Share/logs/inactive_products/inactive_products_{datetime.now().strftime("%m_%d_%y")}.csv"
e_comm_flag_product_log = f"//mainserver/Share/logs/ecomm_flags/move_to_ecomm_enabled_{datetime.now().strftime("%m_%d_%y")}.csv"
always_online_log = f"//mainserver/Share/logs/always_online/always_online_log_{datetime.now().strftime("%m_%d_%y")}.csv"
featured_products = f"//mainserver/Share/logs/featured_products/featured_products_{datetime.now().strftime("%m_%d_%y")}.csv"
unsubscribed_sms = f"//mainserver/Share/logs/sms_unsubscribe/sms_unsubscribe_{datetime.now().strftime("%m_%d_%y")}.csv"
sort_order_log = f"//mainserver/Share/logs/sort_order/sort_order_{datetime.now().strftime("%m_%d_%y")}.csv"
description_log = f"//mainserver/Share/logs/description_log/description_log_{datetime.now().strftime("%m_%d_%y")}.csv"




# Customer Backup
retail_customer_backup = "//mainserver/Share/logs/customer_backup/retail.csv"
wholesale_customer_backup = "//mainserver/Share/logs/customer_backup/wholesale.csv"

# Log Backups
logs = '//mainserver/Share/logs/'
offsite_logs = f'//SettlemyreNAS/admin/Backups/Log Backup/logs_{datetime.now():%m_%d_%y}'

# CP Backups
configuration = "//mainserver/CPSQL.1/Toplevel/Settlemyre/Configuration/"
offsite_configuration = f'//SettlemyreNAS/admin/Backups/Server Backup/configuration_{datetime.now():%m_%d_%y}'

# Backup Location
retail_customer_offsite_backup = "//SettlemyreNAS/admin/Backups/Customer Backup/retail.csv"
wholesale_customer_offsite_backup = "//SettlemyreNAS/admin/Backups/Customer Backup/wholesale.csv"

# SMS logs
first_time_customer_log = "//mainserver/Share/Twilio/Outgoing/Automations/first_time_customer_log.csv"
returning_customer_log = "//mainserver/Share/Twilio/Outgoing/Automations/returning_customer_log.csv"
birthday_coupon_log = "//mainserver/Share/Twilio/Outgoing/Automations/birthday_coupon_log.csv"
landline_log = "//mainserver/Share/Twilio/landline_change_log.csv"
wholesale_log = "//mainserver/Share/Twilio/Outgoing/Automations/wholesale_customer_log.csv"
sms_utility_log = "//mainserver/Share/Twilio/Outgoing/Automations/utility_log.csv"

# Inventory
wholesale_inventory_csv = "//mainserver/Share/logs/inventory/CommercialAvailability.csv"
retail_inventory_csv = "//mainserver/Share/logs/inventory/CurrentAvailability.csv"
stock_notification_log = "//mainserver/Share/logs/stock_notification_log.csv"

# Coupon Log
coupon_creation_log = "//mainserver/Share/logs/coupons/created_coupon_log.csv"
deleted_coupon_log = "//mainserver/Share/logs/coupons/deleted_coupon_log.csv"

# Brands
brand_list = {'KNOCK OUT': 'KNOCKOUT',
              'ESPOMA': 'ESPOMA',
              'DRIFT': 'DRIFT',
              'EVERGREEN': 'EVERGREEN',
              'DADDY PETE': 'DADDY',
              'CORONA': 'CORONA',
              'DARBY': 'DARBY',
              'DRAMM': 'DRAMM',
              'ENCORE': 'ENCORE',
              'PROVEN WINNER': 'PW',
              'FIRST EDITIONS': 'FE',
              'SOUTHERN LIVING': 'SOUTHERN',
              'ENDLESS SUMMER': 'ES'
              }

# coupons
birthday_coupon = "https://settlemyrenursery.com/content/birthdaycoupon.jpg"
five_off_coupon = "https://settlemyrenursery.com/content/5OFFM.jpg"

# Email Template Details
signature_name = "Beth"
signature_title = "Sales Manager"
company_url = "https://settlemyrenursery.com"
company_reviews = ("https://www.google.com/search?q=settlemyre+nursery&oq=settlemyre+nursery&gs_lcrp="
                   "EgZjaHJvbWUqCggAEAAY4wIYgAQyCggAEAAY4wIYgAQyEAgBEC4YrwEYxwEYgAQYjgUyBggCEEUYQDIGCAMQRRg9Mg"
                   "YIBBBFGEEyBggFEEUYQdIBCDIzMThqMGo3qAIAsAIA&sourceid=chrome&ie=UTF-8#lrd=0x8850d134198ffec3:"
                   "0x2c8878dae637a976,1,,,,1:~:text=4.9-,397%20Google%20reviews,-Small%20businessPlant")

company_address_html_1 = company_address_html.split("<br>")[0]
company_address_html_2 = company_address_html.split("<br>")[1]


# Competitor Research
competitor_bank = {
    "Hawksridge": {
        "site": "https://www.hawksridgefarms.com/inventory.php",
        "user_input": "user",
        "pw_input": "pass",
        "username": "Rooster",
        "password": "Hawk23",
        "submit": "submit",
        "log_location": f"//mainserver/Share/logs/research/competitors/"
                        f"Hawksridge_{datetime.now().strftime("%m_%d_%y")}.csv"
    }
}

# Buffer Bank
buffer_bank = {
    "POTTERY": {
        "tier_0": {
            "buffer": 3
        },
        "tier_1": {
            "price": 49.99,
            "buffer": 1,
        },
        "tier_2": {
            "price": 99.99,
            "buffer": 0,
        }
    },

    "TREES": {
        "tier_0": {
            "buffer": 3
        },
        "tier_1": {
            "price": 199.99,
            "buffer": 2,
        },
        "tier_2": {
            "price": 299.99,
            "buffer": 0,
        }
    }
}


bc_api_headers = {
    'X-Auth-Token': big_access_token,
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

test_big_access_token = 'e7ne36ni2qo81e2811ah2yrs4ebbbfc'
test_big_client_id = 'gl03dsshf4m5xoltakbw7wes39a1yt0'
test_big_store_hash = 'xxbbv0cp2l'
test_bc_api_headers = {
    'X-Auth-Token': test_big_access_token,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}
