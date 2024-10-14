from integration.cp_api import OrderAPI
from setup.error_handler import ProcessInErrorHandler
from integration.shopify_api import Shopify
from integration.models.shopify_orders import ShopifyOrder
from setup.email_engine import Email
from database import Database
import traceback
from setup.barcode_engine import generate_barcode
from docxtpl import DocxTemplate, InlineImage
from docx.shared import Mm
from setup import utilities
import os
from datetime import datetime
from setup import creds
from traceback import format_exc as tb


class Order:
    logger = ProcessInErrorHandler.logger
    error_handler = ProcessInErrorHandler.error_handler

    def __init__(
        self,
        order_id: int,
        post: bool = True,
        send_gfc: bool = True,
        print_order: bool = True,
        verbose: bool = False,
    ):
        self.verbose = verbose
        self.order: ShopifyOrder = ShopifyOrder(order_id)
        self.post: bool = post
        self.send_gfc: bool = send_gfc
        self.print_it: bool = print_order

    def __str__(self) -> str:
        return self.order

    def process(self):
        """Process the order by sending gift cards, posting to CP, and printing the order."""
        try:
            if self.post:
                Order.post_order(order=self.order, verbose=self.verbose)

            if self.send_gfc:
                Order.send_gift_cards(self.order)

            if self.print_it:
                Order.print_order(order=self.order)

        except Exception as e:
            Order.error_handler.add_error_v(
                error=f'Error processing order {self.order.id}',
                origin='integration.orders',
                traceback=traceback.format_exc(),
            )

            Order.error_handler.add_error_v(error=str(e), origin='integration.orders')

    @staticmethod
    def post_order(order: ShopifyOrder, verbose: bool = False):
        """Post the order to CP."""
        OrderAPI.process_order(order=order, verbose=verbose)

    @staticmethod
    def send_gift_cards(order: ShopifyOrder):
        """Emails gift cards to customers."""
        for gc in order.gift_card_purchases:
            if not gc.number:
                Order.error_handler.add_error_v('Cannot Send Gift Card - No Code Provided')
                continue

            email = order.email
            if email:
                first_name = order.billing_address.first_name
                last_name = order.billing_address.last_name

                Email.Customer.GiftCard.send(
                    name=f'{first_name.title()} {last_name.title()}',
                    email=email,
                    gc_code=gc.number,
                    amount=gc.amount,
                )
            else:
                Order.error_handler.add_error_v('Cannot Send Gift Card - No Email Provided')

    @staticmethod
    def print_order(order_id: int = None, order: ShopifyOrder = None, test_mode=False):
        """Takes Shopify Order ID ex. 5642506862759 and pulls data, converts to a ShopifyOrder
        and prints a Word Document with the order details. The document is then printed to the default printer."""
        if not order:
            order = Shopify.Order.get(order_id)

        if order.fulfillment_status not in ['UNFULFILLED', 'FULFILLED']:
            Order.logger.info(f'Order {order_id} - Status: {order.fulfillment_status}. Skipping print.')
            return

        if order.is_gift_card_only:
            OrderAPI.logger.info(f'Order {order_id} contains only gift cards. Skipping print.')
            return

        cust_no = OrderAPI.get_customer_number(order)

        if not cust_no:
            first_name = 'Web'
            last_name = 'Customer'
            email = 'No Email'
            phone = 'No Phone'
        else:
            first_name = order.customer.first_name or 'Web'
            last_name = order.customer.last_name or 'Customer'
            email = order.email or 'No Email'
            phone = order.get_phone(order=order) or 'No Phone'

        # Get Product List
        product_list = []
        for x in order.line_items:
            product_details = {
                'sku': x.sku,
                'name': Database.CP.Product.get_long_descr(x.sku),
                'qty': x.quantity,
                'base_price': x.retail_price,
                'base_total': x.retail_price,
            }

            product_list.append(product_details)

        OrderAPI.logger.info('Creating barcode')

        try:
            barcode_file = generate_barcode(data=order.id)
        except Exception as err:
            error_type = 'barcode'
            OrderAPI.error_handler.add_error_v(error=f'Error ({error_type}): {err}', origin='Order - Barcode')
            return

        OrderAPI.logger.success(f'Creating barcode - Success at {datetime.now():%H:%M:%S}')

        try:
            date = f'{order.date_created:%m/%d/%Y}'  # ex. 04/24/2024
            time = f'{order.date_created:%I:%M:%S %p}'  # ex. 02:34:24 PM

            doc = DocxTemplate('./templates/order_print_template.docx')

            barcode = InlineImage(doc, barcode_file, height=Mm(15))  # width in mm

            context = {
                # Company Details
                'company_name': creds.Company.name,
                'co_address': creds.Company.address,
                'co_phone': creds.Company.phone,
                # Order Details
                'order_number': order.id,
                'order_date': date,
                'order_time': time,
                'order_subtotal': order.subtotal,
                'order_shipping': order.base_shipping_cost,
                'order_total': order.total,
                'cust_no': cust_no,
                # Customer Billing
                'cb_name': first_name + ' ' + last_name,
                'cb_phone': phone,
                'cb_email': email,
                'cb_street': order.billing_address.address_1 or '',
                'cb_city': order.billing_address.city or '',
                'cb_state': order.billing_address.province or '',
                'cb_zip': order.billing_address.zip or '',
                # Customer Shipping
                'shipping_method': 'Delivery' if order.base_shipping_cost > 0 else 'Pickup',
                'cs_name': (order.shipping_address.first_name or '')
                + ' '
                + (order.shipping_address.last_name or ''),
                'cs_phone': order.shipping_address.phone or '',
                'cs_email': order.shipping_address.email or '',
                'cs_street': order.shipping_address.address_1 or '',
                'cs_city': order.shipping_address.city or '',
                'cs_state': order.shipping_address.province or '',
                'cs_zip': order.shipping_address.zip or '',
                # Product Details
                'number_of_items': len(order.line_items),
                'ticket_notes': order.customer_message or '',
                'products': product_list,
                'coupon_code': ', '.join(order.coupon_codes),
                'coupon_discount': order.total_discount,
                'loyalty': order.store_credit_amount,
                'gc_amount': 0,  # Not implemented
                'barcode': barcode,
                'status': order.fulfillment_status,
            }

            doc.render(context)
            ticket_name = f"ticket_{order.id}_{datetime.now().strftime("%m_%d_%y_%H_%M_%S")}.docx"
            file_path = creds.Company.ticket_location + '/' + ticket_name
            doc.save(file_path)

        except Exception as err:
            error_type = 'Word Document'
            OrderAPI.error_handler.add_error_v(
                error=f'Error ({error_type}): {err}', origin='Order-Word Document', traceback=tb()
            )
            return

        OrderAPI.logger.success(f'Creating Word Document - Success at {datetime.now():%H:%M:%S}')

        try:
            # Print the file to default printer
            if test_mode:
                OrderAPI.logger.info('Test Mode: Skipping Print')
            else:
                # convert file_path to raw string
                file_path = utilities.convert_path_to_raw(file_path)
                os.startfile(file_path, 'print')  # print the file to default printer
                os.remove(barcode_file)  # remove barcode file after printing
        except Exception as err:
            error_type = 'Printing'
            OrderAPI.error_handler.add_error_v(f'Error ({error_type}): {err}', origin='Design - Printing')
        else:
            OrderAPI.logger.success(f'Printing - Success at {datetime.now():%H:%M:%S}')

    @staticmethod
    def delete(tkt_no: str):
        """Delete an order from CP by ticket number."""
        doc_id = Database.CP.OpenOrder.get_doc_id(tkt_no)
        customer = Database.CP.OpenOrder.get_customer(doc_id)
        points_earned = Database.CP.Loyalty.get_points_earned(doc_id)
        points_redeeemed = Database.CP.Loyalty.get_points_redeemed(doc_id)

        OrderAPI.delete(ticket_no=tkt_no)

        if points_earned:
            Database.CP.Loyalty.remove_points(cust_no=customer, points=points_earned)
        if points_redeeemed:
            Database.CP.Loyalty.add_points(cust_no=customer, points=points_redeeemed)


class OrderProcessor:
    def __init__(self, order_ids: list[str | int] = []):
        self.order_ids = order_ids

    def process(self):
        orders = [Order(order_id) for order_id in self.order_ids]
        for order in orders:
            order.process()


if __name__ == '__main__':
    # Order.delete(tkt_no='S1158')
    Order(5717619933351, print_order=False, send_gfc=False).process()
