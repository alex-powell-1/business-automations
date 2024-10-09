from config_file import config_data
import platform
import os


class Config:
    docker: dict = config_data['docker']
    integrator: dict = config_data['integrator']
    api: dict = config_data['api']
    company: dict = config_data['company']
    sql: dict = config_data['sql']
    shopify: dict = config_data['shopify']
    keys: dict = config_data['keys']
    site: dict = shopify['live_site']
    counterpoint: dict = config_data['counterpoint']
    consumers: dict = config_data['consumers']
    backups: dict = config_data['backups']
    marketing: dict = config_data['marketing']


class API:
    endpoint: str = Config.api['endpoint']
    server_name: str = Config.api['server_name']
    public_files: str = endpoint + Config.api['routes']['file_server']

    if os.path.exists(Config.docker['public_files']):
        # Docker Container
        public_files_local_path = Config.docker['public_files']
    else:
        # Local Development
        if platform.system() == 'Windows':
            public_files_local_path = f'//{server_name}/' + Config.api['public_files_local']
        else:
            public_files_local_path = '/Volumes/' + Config.api['public_files_local']

    port: int = Config.api['port']
    default_rate: int = '100/second'

    class Route:
        """API Routes"""

        file_server = Config.api['routes']['file_server']
        design = Config.api['routes']['design']
        design_admin = Config.api['routes']['design_admin']
        stock_notify = Config.api['routes']['stock_notify']
        newsletter = Config.api['routes']['newsletter']
        sms = Config.api['routes']['sms']
        qr = Config.api['routes']['qr']
        token = Config.api['routes']['token']
        commercial_availability = Config.api['routes']['commercial_availability']
        retail_availability = Config.api['routes']['retail_availability']
        unsubscribe = Config.api['routes']['unsubscribe']
        subscribe = Config.api['routes']['subscribe']

        class Shopify:
            """Shopify Webhook Routes"""

            order_create = Config.api['routes']['shopify']['order_create']
            refund_create = Config.api['routes']['shopify']['refund_create']
            draft_create = Config.api['routes']['shopify']['draft_create']
            draft_update = Config.api['routes']['shopify']['draft_update']
            customer_create = Config.api['routes']['shopify']['customer_create']
            customer_update = Config.api['routes']['shopify']['customer_update']
            product_update = Config.api['routes']['shopify']['product_update']
            variant_out_of_stock = Config.api['routes']['shopify']['variant_out_of_stock']
            collection_update = Config.api['routes']['shopify']['collection_update']


class Integrator:
    """Integrator Configuration"""

    if os.path.exists(Config.docker['logs']):
        # Docker Container
        logs = Config.docker['logs']
    else:
        # Local Development
        if platform.system() == 'Windows':
            logs = f'//{API.server_name}/' + Config.integrator['logs']
        else:
            logs = '/Volumes/' + Config.integrator['logs']

    directory = Config.integrator['directory']
    title = Config.integrator['title']
    authors = Config.integrator['authors']
    version = Config.integrator['version']
    max_workers: int = Config.integrator['max_workers']  # Thread Pool
    day_start: int = Config.integrator['day_start']
    day_end: int = Config.integrator['day_end']
    int_day_run_interval: int = Config.integrator['integrator_day_run_interval']  # Minutes
    int_night_run_interval = Config.integrator['integrator_night_run_interval']  # Minutes
    inv_day_run_interval: int = Config.integrator['inventory_day_run_interval']  # Seconds
    inv_night_run_interval: int = Config.integrator['inventory_night_run_interval']  # Seconds
    promotion_sync: bool = Config.integrator['promotion_sync']
    customer_sync: bool = Config.integrator['customer_sync']
    subscriber_sync: bool = Config.integrator['subscriber_sync']
    catalog_sync: bool = Config.integrator['catalog_sync']
    collection_sorting: bool = Config.integrator['collection_sorting']
    inventory_sync: bool = Config.integrator['inventory_sync']
    sms_sync_keyword: str = Config.integrator['sms_sync_keyword']
    verbose_logging: bool = Config.integrator['verbose_logging']
    default_image_url: str = Config.integrator['default_image_url']
    set_missing_image_active: bool = Config.integrator['missing_image_active']


class SQL:
    """SQL Server Configuration"""

    SERVER: str = Config.sql['address']
    DATABASE: str = Config.sql['database']
    PORT: int = Config.sql['port']
    USERNAME: str = Config.sql['db_username']
    PASSWORD: str = Config.sql['db_password']


# Company
class Company:
    name = Config.company['name']
    phone = Config.company['phone']
    product_brand = Config.company['product_brand']
    review_link = Config.company['review_link']
    reviews = Config.company['reviews']
    address = Config.company['address']
    address_html = Config.company['address_html']
    address_html_1 = address_html.split('<br>')[0]
    address_html_2 = address_html.split('<br>')[1]
    url = Config.company['url']
    hours = Config.company['hours']['month']
    logo = Config.company['logo']
    product_images = Config.company['item_images']
    category_images = Config.company['category_images']
    brand_images = Config.company['brands']['images']
    brand_list = Config.company['brands']['list']
    barcodes = Config.company['barcodes']
    stock_buffers = Config.company['stock_buffers']
    binding_id_format = Config.company['binding_id_format']
    network_notification_phone = Config.company['network_notification_phone']
    recaptcha_secret = Config.api['google_recaptcha']['secret_key']  # Google Recaptcha Secret Key
    design_admin_key = Config.company['design_admin_key']  # Design Admin Key - alternative to Google Recaptcha
    competitors = config_data['research']['competitors']
    ticket_location = Config.company['ticket_location']
    commercial_availability_pw = Config.company['commercial_availability_pw']
    commercial_inventory_csv = f'{API.public_files}/availability/CommercialAvailability.csv'
    retail_inventory_csv = f'{API.public_files}/availability/CurrentAvailability.csv'
    staff: dict = Config.company['staff']


class Counterpoint:
    class Categories:
        on_sale = Config.counterpoint['categories']['sale']

    class API:
        """NCR Counterpoint API"""

        user = Config.counterpoint['api']['cp_api_user']
        key = Config.counterpoint['api']['cp_api_key']
        server = Config.counterpoint['api']['cp_api_server']
        order_server = Config.counterpoint['api']['cp_api_order_server']


class Table:
    """Tables for SQL Queries"""

    qr = Config.company['tables']['qr']
    qr_activity = Config.company['tables']['qr_activity']
    sms = Config.company['tables']['sms']
    sms_event = Config.company['tables']['sms_event']
    sms_subsribe = Config.company['tables']['sms_subscribe']
    design_leads = Config.company['tables']['design_leads']
    stock_notify = Config.company['tables']['stock_notify']
    vi_stock_notify = Config.company['tables']['vi_stock_notify']
    newsletter = Config.company['tables']['newsletter']
    email_list = Config.company['tables']['email_list']
    vi_newsletter = Config.company['tables']['vi_newsletter']

    class CP:
        """Counterpoint Tables"""

        item_prices = config_data['counterpoint']['tables']['item_prices']
        customers = config_data['counterpoint']['tables']['customers']
        customer_ship_addresses = config_data['counterpoint']['tables']['customer_ship_adrs']
        open_orders = config_data['counterpoint']['tables']['open_orders']
        closed_orders = config_data['counterpoint']['tables']['closed_orders']
        discounts = config_data['counterpoint']['tables']['discounts']

        class Item:
            """Counterpoint Item Table"""

            table = config_data['counterpoint']['tables']['items']['table']

            class Column:
                """Counterpoint Item Columns"""

                __col = Config.counterpoint['tables']['items']['columns']
                item_no = __col['item_no']
                binding_id = __col['binding_id']
                is_parent = __col['is_parent']
                web_enabled = __col['web_enabled']
                web_visible = __col['web_visible']
                variant_name = __col['variant_name']
                tags = __col['tags']
                weight = __col['weight']
                sort_order = __col['sort_order']
                brand = __col['brand']
                web_title = __col['web_title']
                meta_title = __col['meta_title']
                meta_description = __col['meta_description']
                alt_text_1 = __col['alt_text_1']
                alt_text_2 = __col['alt_text_2']
                alt_text_3 = __col['alt_text_3']
                alt_text_4 = __col['alt_text_4']
                videos = __col['videos']
                featured = __col['featured']
                in_store_only = __col['in_store_only']
                is_preorder_item = __col['is_preorder_item']
                preorder_message = __col['preorder_message']
                preorder_release_date = __col['preorder_release_date']
                is_on_sale = __col['is_on_sale']
                sale_description = __col['sale_description']
                botanical_name = __col['botanical_name']
                plant_type = __col['plant_type']
                buffer = __col['buffer']
                promotion_date_exp = __col['promotion_dt_exp']
                is_new = __col['new']
                is_back_in_stock = __col['back_in_stock']

            class HTMLDescription:
                table = config_data['counterpoint']['tables']['html_description']

        class Customers:
            """Counterpoint Customer Table"""

            table = config_data['counterpoint']['tables']['customers']['table']

            class Column:
                __col = config_data['counterpoint']['tables']['customers']['columns']
                number = __col['number']
                first_name = __col['first_name']
                last_name = __col['last_name']
                email_1 = __col['email_1']
                email_2 = __col['email_2']
                mobile_phone_1: str = __col['mobile_phone_1']
                phone_1 = __col['phone_1']
                mobile_phone_2 = __col['mobile_phone_2']
                phone_2 = __col['phone_2']
                sms_1_is_subscribed = __col['sms_1_is_subscribed']
                sms_1_opt_in_date = __col['sms_1_opt_in_date']
                sms_1_last_maint_date = __col['sms_1_last_maint_date']
                sms_2_is_subscribed = __col['sms_2_is_subscribed']
                sms_2_opt_in_date = __col['sms_2_opt_in_date']
                sms_2_last_maint_date = __col['sms_2_last_maint_date']
                email_1_is_subscribed = __col['email_1_is_subscribed']
                email_1_opt_in_date = __col['email_1_opt_in_date']
                email_1_last_maint_date = __col['email_1_last_maint_date']
                email_2_is_subscribed = __col['email_2_is_subscribed']
                email_2_opt_in_date = __col['email_2_opt_in_date']
                email_2_last_maint_date = __col['email_2_last_maint_date']
                is_ecomm_customer = __col['is_ecomm_customer']

    class Middleware:
        """Middleware Tables"""

        products = Config.site['tables']['products']
        collections = Config.site['tables']['collections']
        images = Config.site['tables']['images']
        videos = Config.site['tables']['videos']
        customers = Config.site['tables']['customers']
        orders = Config.site['tables']['orders']
        draft_orders = Config.site['tables']['draft_orders']
        gift_certificates = Config.site['tables']['gift_certificates']
        promotions = Config.site['tables']['promotions']
        promotion_lines_bogo = Config.site['tables']['promotion_lines_bogo']
        promotion_lines_fixed = Config.site['tables']['promotion_lines_fixed']
        discounts = Config.site['tables']['discounts']
        discounts_view = Config.site['tables']['discounts_view']
        metafields = Config.site['tables']['metafields']
        webhooks = Config.site['tables']['webhooks']


class Twilio:
    phone_number = Config.keys['twilio']['twilio_phone_number']
    sid = Config.keys['twilio']['twilio_account_sid']
    token = Config.keys['twilio']['twilio_auth_token']


class Sheety:
    design_url = Config.keys['sheety']['design_url']
    token = Config.keys['sheety']['token']


class ConstantContact:
    key = Config.keys['constant_contact']['api_key']
    client_secret = Config.keys['constant_contact']['client_secret']


class Shopify:
    shop_url = Config.site['shop_url']
    admin_token = Config.site['token']
    secret_key = Config.site['secret_key']
    location_1387 = Config.site['locations']['1387']

    class SalesChannel:
        online_store = Config.site['channels']['online_store']
        pos = Config.site['channels']['pos']
        shop = Config.site['channels']['shop']
        inbox = Config.site['channels']['inbox']
        google = Config.site['channels']['google']

    class Menu:
        main = Config.site['menus']['main']['id']

    class Location:
        n2 = Config.site['locations']['1387']

    class Metafield:
        class Namespace:
            class Product:
                specification = Config.site['metafields']['namespace']['product']['specification']
                status = Config.site['metafields']['namespace']['product']['status']

            class Variant:
                specifiation = Config.site['metafields']['namespace']['variant']['specification']

            class Customer:
                customer = Config.site['metafields']['namespace']['customer']['customer']

        class Product:
            growing_zone = Config.site['metafields']['products']['growing_zone']
            growing_zone_list = Config.site['metafields']['products']['growing_zone_list']
            botanical_name = Config.site['metafields']['products']['botanical_name']
            plant_type = Config.site['metafields']['products']['plant_type']
            height = Config.site['metafields']['products']['height']
            width = Config.site['metafields']['products']['width']
            light_requirements = Config.site['metafields']['products']['light_requirements']
            color = Config.site['metafields']['products']['color']
            size = Config.site['metafields']['products']['size']
            bloom_season = Config.site['metafields']['products']['bloom_season']
            bloom_color = Config.site['metafields']['products']['bloom_color']
            features = Config.site['metafields']['products']['features']
            new = Config.site['metafields']['products']['new']
            back_in_stock = Config.site['metafields']['products']['back_in_stock']

        class Variant:
            size = Config.site['metafields']['variants']['size']

        class Customer:
            customer_number = Config.site['metafields']['customers']['customer_number']
            category = Config.site['metafields']['customers']['category']
            birth_month = Config.site['metafields']['customers']['birth_month']
            spouse_birth_month = Config.site['metafields']['customers']['spouse_birth_month']
            wholesale_price_tier = Config.site['metafields']['customers']['wholesale_price_tier']


class Consumer:
    """RabbitMQ Consumer Queue Names"""

    orders = Config.consumers['orders']
    draft_create = Config.consumers['draft_create']
    draft_update = Config.consumers['draft_update']
    customer_update = Config.consumers['customer_update']
    product_update = Config.consumers['product_update']
    design_lead_form = Config.consumers['design_info']
    sync_on_demand = Config.consumers['sync_on_demand']
    restart_services = Config.consumers['restart_services']


class BatchFiles:
    """Batch File Configuration"""

    directory = config_data['batch_files']['directory']
    sync = config_data['batch_files']['sync']


# Gmail Account
class Gmail:
    class Sales:
        username = Config.keys['gmail']['sales']['username']
        password = Config.keys['gmail']['sales']['password']


class Reports:
    """Report Configuration"""

    class Administrative:
        enabled: bool = config_data['reports']['administrative']['enabled']
        hour: int = config_data['reports']['administrative']['hour']
        minute: int = config_data['reports']['administrative']['minute']
        sender: str = config_data['reports']['administrative']['sender']
        recipients: list[str] = config_data['reports']['administrative']['recipients']

    class Item:
        enabled: bool = config_data['reports']['item']['enabled']
        hour: int = config_data['reports']['item']['hour']
        minute: int = config_data['reports']['item']['minute']
        sender: str = config_data['reports']['item']['sender']
        recipients: list[str] = config_data['reports']['item']['recipients']

    class LowStock:
        enabled: bool = config_data['reports']['low_stock']['enabled']
        hour: int = config_data['reports']['low_stock']['hour']
        minute: int = config_data['reports']['low_stock']['minute']
        sender: str = config_data['reports']['low_stock']['sender']
        recipients: list[str] = config_data['reports']['low_stock']['recipients']

    class MarketingLeads:
        enabled: bool = config_data['reports']['marketing_leads']['enabled']
        hour: int = config_data['reports']['marketing_leads']['hour']
        minute: int = config_data['reports']['marketing_leads']['minute']
        sender: str = config_data['reports']['marketing_leads']['sender']
        recipients: list[str] = config_data['reports']['marketing_leads']['recipients']


# SMS Automations
class SMSAutomations:
    enabled: bool = config_data['sms']['automations']['enabled']  # True will run automations
    test_mode: bool = config_data['sms']['automations']['test_mode']  # True will run automations in test mode

    class TestCustomer:
        enabled: bool = config_data['sms']['automations']['test_customer']['enabled']
        cust_list: list[str] = config_data['sms']['automations']['test_customer']['cust_list']

    class Campaigns:
        class Birthday:
            title = 'Birthday Text'
            enabled: bool = config_data['sms']['automations']['campaigns']['birthday']['enabled']
            day = config_data['sms']['automations']['campaigns']['birthday']['day']
            hour = config_data['sms']['automations']['campaigns']['birthday']['hour']
            minute = config_data['sms']['automations']['campaigns']['birthday']['minute']

        class FTC1:
            title = 'First Time Cust Text 1'
            enabled: bool = config_data['sms']['automations']['campaigns']['ftc_1']['enabled']
            hour = config_data['sms']['automations']['campaigns']['ftc_1']['hour']
            minute = config_data['sms']['automations']['campaigns']['ftc_1']['minute']

        class FTC2:
            title = 'First Time Cust Text 2'
            enabled: bool = config_data['sms']['automations']['campaigns']['ftc_2']['enabled']
            hour = config_data['sms']['automations']['campaigns']['ftc_2']['hour']
            minute = config_data['sms']['automations']['campaigns']['ftc_2']['minute']

        class FTC3:
            title = 'First Time Cust Text 3'
            enabled: bool = config_data['sms']['automations']['campaigns']['ftc_3']['enabled']
            hour = config_data['sms']['automations']['campaigns']['ftc_3']['hour']
            minute = config_data['sms']['automations']['campaigns']['ftc_3']['minute']

        class RC1:
            title = 'Returning Cust Text 1'
            enabled: bool = config_data['sms']['automations']['campaigns']['rc_1']['enabled']
            hour = config_data['sms']['automations']['campaigns']['rc_1']['hour']
            minute = config_data['sms']['automations']['campaigns']['rc_1']['minute']

        class RC2:
            title = 'Returning Cust Text 2'
            enabled: bool = config_data['sms']['automations']['campaigns']['rc_2']['enabled']
            hour = config_data['sms']['automations']['campaigns']['rc_2']['hour']
            minute = config_data['sms']['automations']['campaigns']['rc_2']['minute']

        class RC3:
            title = 'Returning Cust Text 3'
            enabled: bool = config_data['sms']['automations']['campaigns']['rc_3']['enabled']
            hour = config_data['sms']['automations']['campaigns']['rc_3']['hour']
            minute = config_data['sms']['automations']['campaigns']['rc_3']['minute']

        class Wholesale1:
            title = 'Wholesale Text 1'
            enabled: bool = config_data['sms']['automations']['campaigns']['wholesale_1']['enabled']
            hour = config_data['sms']['automations']['campaigns']['wholesale_1']['hour']
            minute = config_data['sms']['automations']['campaigns']['wholesale_1']['minute']


class Logs:
    server = f'{Integrator.logs}/server'
    process_in = f'{Integrator.logs}/integration/process_in'
    process_out = f'{Integrator.logs}/integration/process_out'
    sms = f'{Integrator.logs}/sms'
    design_leads = f'{Integrator.logs}/design_leads'
    sms_events = f'{Integrator.logs}/sms'
    scheduled_tasks = f'{Integrator.logs}/scheduled_tasks'

    webhooks_product_update = f'{Integrator.logs}/webhooks/product_update'
    webhooks_order_create = f'{Integrator.logs}/webhooks/order_create'
    webhooks_refund_create = f'{Integrator.logs}/webhooks/refund_create'
    webhooks_draft_create = f'{Integrator.logs}/webhooks/draft_create'
    webhooks_draft_update = f'{Integrator.logs}/webhooks/draft_update'
    webhooks_customer_create = f'{Integrator.logs}/webhooks/customer_create'
    webhooks_customer_update = f'{Integrator.logs}/webhooks/customer_update'

    class Webhooks:
        class Shopify:
            order_create = f'{Integrator.logs}/webhooks/shopify/order_create'
            refund_create = f'{Integrator.logs}/webhooks/shopify/refund_create'
            draft_create = f'{Integrator.logs}/webhooks/shopify/draft_create'
            draft_update = f'{Integrator.logs}/webhooks/shopify/draft_update'
            customer_create = f'{Integrator.logs}/webhooks/shopify/customer_create'
            customer_update = f'{Integrator.logs}/webhooks/shopify/customer_update'
            product_update = f'{Integrator.logs}/webhooks/shopify/product_update'
            variant_out_of_stock = f'{Integrator.logs}/webhooks/shopify/variant_out_of_stock'
            collection_update = f'{Integrator.logs}/webhooks/shopify/collection_update'


class Backups:
    class Customer:
        retail = Config.backups['customer']['retail']
        wholesale = Config.backups['customer']['wholesale']

    class Config:
        source = Config.backups['config']['src']
        destination = Config.backups['config']['dst']


class Coupon:
    birthday = f'{API.public_files}/sms/birthdaycoupon.jpg'
    five_off = f'{API.public_files}/sms/5OFFM.jpg'


class BigCommerce:
    big_access_token = Config.keys['big_commerce']['access_token']
    client_id = Config.keys['big_commerce']['client_id']
    store_hash = Config.keys['big_commerce']['store_hash']
    webhook_orders = Config.keys['big_commerce']['webhooks']['order']
    webhook_refund = Config.keys['big_commerce']['webhooks']['refund']
    api_headers = {
        'X-Auth-Token': big_access_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }


class Marketing:
    class DesignLeadForm:
        lead_recipient = Config.marketing['design_lead_form']['lead_recipient']
        test_recipient = Config.marketing['design_lead_form']['test_recipient']
        pdf_attachment = Config.marketing['design_lead_form']['pdf_attachment']
        pdf_name = Config.marketing['design_lead_form']['pdf_name']
        email_subject = Config.marketing['design_lead_form']['email_subject']
        service = Config.marketing['design_lead_form']['service']
        signature_name = Config.marketing['design_lead_form']['signature_name']
        signature_title = Config.marketing['design_lead_form']['signature_title']
        list_items = Config.marketing['design_lead_form']['list_items']
        schema = Config.marketing['design_lead_form']['schema']

    class Newsletter:
        schema = Config.marketing['newsletter']['schema']

    class StockNotification:
        offer: str = Config.marketing['stock_notification']['offer']  # description
        discount: int = Config.marketing['stock_notification']['discount']  # $ Amount
        min_amt: int = Config.marketing['stock_notification']['min_amt']  # $ Amount
        exclusions: list[str] = Config.marketing['stock_notification']['exclusions']
