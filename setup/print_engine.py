from database import Database as db
from docxtpl import DocxTemplate, InlineImage
from datetime import datetime as dt
from setup.error_handler import LeadFormErrorHandler, ProcessInErrorHandler
from docx.shared import Mm
from integration.shopify_api import Shopify
import os
import time
from setup import creds
from setup.barcode_engine import generate_barcode
from setup import utilities
from datetime import datetime
from product_tools.products import Product
from traceback import format_exc as tb
from setup.order_engine import utc_to_local
from integration.cp_api import OrderAPI
from database import Database

test_mode = False


class Printer:
    logger = ProcessInErrorHandler.logger
    error_handler = ProcessInErrorHandler.error_handler

    def print_design_lead(first_name, year, month, day, delete=True, test_mode=False):
        """Takes a first name and date and prints a Word Document with the lead details.
        The document is then retrieved from the database and printed to the default printer."""

        query = f"""
        SELECT * FROM SN_LEADS_DESIGN
        WHERE FST_NAM = '{first_name}' and DATE > '{year}-{month}-{day}'
        """
        response = db.query(query)
        if not response:
            print('No leads found')
        else:
            for row in response:
                print(f'Found lead for {row[3]} {row[4]}')
                key_input = input('press y to continue')
                if key_input != 'y':
                    return
                date = f'{row[1]:%m-%d-%Y %H:%M:%S}'

                doc = DocxTemplate('./templates/design_lead/lead_print_template.docx')
                # Format the interested_in field

                interested_in = []
                if row[7]:
                    interested_in.append('Sketch and Go')
                if row[8]:
                    interested_in.append('Scaled Drawing')
                if row[9]:
                    interested_in.append('Digital Rendering')
                if row[10]:
                    interested_in.append('On-Site Consultation')
                if row[11]:
                    interested_in.append('Delivery and Placement')
                if row[12]:
                    interested_in.append('Professional Installation')

                # Format the address
                address = f'{row[14].title()} {row[15].title()}, {row[16].upper()} {row[17]}'
                context = {
                    # Product Details
                    'date': date,
                    'name': row[3].title() + ' ' + row[4].title(),
                    'email': row[5],
                    'phone': row[6],
                    'interested_in': interested_in,
                    'timeline': row[13],
                    'address': address,
                    'comments': row[18].replace('""', '"'),
                }

                doc.render(context)
                ticket_name = f'lead_{dt.now():%H_%M_%S}.docx'
                # Save the rendered file for printing
                doc.save(f'./{ticket_name}')
                # Print the file to default printer
                LeadFormErrorHandler.logger.info('Printing Word Document')
                if test_mode:
                    LeadFormErrorHandler.logger.info('Test Mode: Skipping Print')
                else:
                    if test_mode:
                        LeadFormErrorHandler.logger.info('Test Mode: Skipping Print')
                    else:
                        ticket_name = utilities.convert_path_to_raw(ticket_name)
                        os.startfile(ticket_name, 'print')
                if delete:
                    # Delay while print job executes
                    time.sleep(4)
                    LeadFormErrorHandler.logger.info('Deleting Word Document')
                    os.remove(ticket_name)

    def print_order(order_id, test_mode=False):
        """Takes Shopify Order ID ex. 5642506862759 and pulls data, converts to a BigCommerce Order (for parsing)
        and prints a Word Document with the order details. The document is then printed to the default printer."""
        order = Shopify.Order.as_bc_order(order_id)
        cust_no = OrderAPI.get_cust_no(order)

        # Get Basic Customer Info from Counterpoint if available
        customer = Database.CP.Customer(cust_no)
        if not customer.CUST_NO:
            Printer.logger.warn(f'Customer {cust_no} not found in Counterpoint')
            first_name = 'Web'
            last_name = 'Customer'
            email = 'No Email'
            phone = 'No Phone'
        else:
            first_name = customer.FST_NAM or 'Web'
            last_name = customer.LST_NAM or 'Customer'
            email = customer.EMAIL_ADRS_1 or 'No Email'
            phone = customer.PHONE_1 or 'No Phone'

        # Get Product List
        products = order['products']['url']
        product_list = []
        gift_card_only = True
        for x in products:
            if x['type'] == 'physical':
                gift_card_only = False

            item = Product(x['sku'])

            product_details = {
                'sku': item.item_no,
                'name': item.descr,
                'qty': x['quantity'],
                'base_price': x['base_price'],
                'base_total': x['base_total'],
            }

            product_list.append(product_details)
        # Filter out orders that only contain gift cards
        if gift_card_only:
            Printer.logger.info(f'Order {order_id} contains only gift cards. Skipping print.')
        else:
            Printer.logger.info('Creating barcode')
            try:
                barcode_file = generate_barcode(data=order['id'])
            except Exception as err:
                error_type = 'barcode'
                Printer.error_handler.add_error_v(error=f'Error ({error_type}): {err}', origin='Design - Barcode')
            else:
                Printer.logger.success(f'Creating barcode - Success at {datetime.now():%H:%M:%S}')

            try:
                bc_date = order['date_created']
                bc_date = datetime.strptime(bc_date, '%Y-%m-%dT%H:%M:%SZ')
                # convert to local time
                bc_date = utc_to_local(bc_date)
                # Format Date and Time
                # dt_date = utils.parsedate_to_datetime(bc_date)
                date = f'{bc_date:%m/%d/%Y}'  # ex. 04/24/2024
                time = f'{bc_date:%I:%M:%S %p}'  # ex. 02:34:24 PM

                doc = DocxTemplate('./templates/order_print_template.docx')

                barcode = InlineImage(doc, barcode_file, height=Mm(15))  # width in mm

                context = {
                    # Company Details
                    'company_name': creds.Company.name,
                    'co_address': creds.Company.address,
                    'co_phone': creds.Company.phone,
                    # Order Details
                    'order_number': order['id'],
                    'order_date': date,
                    'order_time': time,
                    'order_subtotal': float(order['subtotal_inc_tax']),
                    'order_shipping': float(order['base_shipping_cost']),
                    'order_total': float(order['total_inc_tax']),
                    'cust_no': str(cust_no),
                    # Customer Billing
                    'cb_name': first_name + ' ' + last_name,
                    'cb_phone': phone,
                    'cb_email': email,
                    'cb_street': order['billing_address']['street_1'] or '',
                    'cb_city': order['billing_address']['city'] or '',
                    'cb_state': order['billing_address']['state'] or '',
                    'cb_zip': order['billing_address']['zip'] or '',
                    # Customer Shipping
                    'shipping_method': 'Delivery' if float(order['base_shipping_cost']) > 0 else 'Pickup',
                    'cs_name': (order['shipping_addresses']['url'][0]['first_name'] or '')
                    + ' '
                    + (order['shipping_addresses']['url'][0]['last_name'] or ''),
                    'cs_phone': order['shipping_addresses']['url'][0]['phone'] or '',
                    'cs_email': order['shipping_addresses']['url'][0]['email'] or '',
                    'cs_street': order['shipping_addresses']['url'][0]['street_1'] or '',
                    'cs_city': order['shipping_addresses']['url'][0]['city'] or '',
                    'cs_state': order['shipping_addresses']['url'][0]['state'] or '',
                    'cs_zip': order['shipping_addresses']['url'][0]['zip'] or '',
                    # Product Details
                    'number_of_items': order['items_total'],
                    'ticket_notes': order['customer_message'] or '',
                    'products': product_list,
                    'coupon_code': ', '.join(order['order_coupons']),
                    'coupon_discount': float(order['coupons']['url'][0]['amount'])
                    if len(order['coupons']['url']) > 0
                    else 0,
                    'loyalty': float(order['store_credit_amount']),
                    'gc_amount': float(order['gift_certificate_amount'])
                    if 'gift_certificate_amount' in order
                    else 0,
                    'barcode': barcode,
                    'status': order['status'],
                }

                doc.render(context)
                ticket_name = f"ticket_{order['id']}_{datetime.now().strftime("%m_%d_%y_%H_%M_%S")}.docx"
                file_path = creds.Company.ticket_location + '/' + ticket_name
                doc.save(file_path)

            except Exception as err:
                error_type = 'Word Document'
                Printer.error_handler.add_error_v(
                    error=f'Error ({error_type}): {err}', origin='Design - Word Document', traceback=tb()
                )

            else:
                Printer.logger.success(f'Creating Word Document - Success at {datetime.now():%H:%M:%S}')
                try:
                    # Print the file to default printer
                    if test_mode:
                        Printer.logger.info('Test Mode: Skipping Print')
                    else:
                        # convert file_path to raw string
                        file_path = utilities.convert_path_to_raw(file_path)
                        os.startfile(file_path, 'print')  # print the file to default printer
                        os.remove(barcode_file)  # remove barcode file after printing
                except Exception as err:
                    error_type = 'Printing'
                    Printer.error_handler.add_error_v(f'Error ({error_type}): {err}', origin='Design - Printing')
                else:
                    Printer.logger.success(f'Printing - Success at {datetime.now():%H:%M:%S}')


if __name__ == '__main__':
    Printer.print_order(5687251632295)
    # generate_barcode(data='as', filename='barcode2')
