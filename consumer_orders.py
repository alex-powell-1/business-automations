import os
import sys
from time import sleep
from datetime import datetime
from email import utils

import pandas
import pika
from docx.shared import Mm
from docxtpl import DocxTemplate, InlineImage

from setup import barcode_engine
from setup import creds, product_engine, log_engine, query_engine
from setup.order_engine import Order, utc_to_local

from integration.orders import Order as BCOrder


class RabbitMQConsumer:
    def __init__(self, queue_name, host="localhost"):
        self.queue_name = queue_name
        self.host = host
        self.connection = None
        self.channel = None

    def connect(self):
        parameters = pika.ConnectionParameters(self.host)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def callback(self, ch, method, properties, body):
        # luke, we will remove this after we are done testing
        log_file = open(creds.create_log(datetime.now(), "order"), "a")
        now = datetime.now()
        order_id = body.decode()

        # Log incoming order for debugging
        error_data = [[str(now)[:-7], order_id]]
        df = pandas.DataFrame(error_data, columns=["date", "order_id"])
        log_engine.write_log(df, log_location=creds.webhook_order_log)

        # /luke

        # Create order object
        print(f"Beginning processing for Order #{order_id}", file=log_file)
        try:
            # Give CC Processor time to complete capture of CC info and process
            sleep(5)  # <---- This is to give payment processor time to complete

            print(f"Getting Order Details", file=log_file)

            order = Order(order_id)

            # Filter out DECLINED payments
            if order.status_id == 11:
                # Add order to SQL Database. Datestamp and status are added by default.
                query = f"INSERT INTO SN_ORDERS (ORDER_NO) VALUES ({order_id})"
                db = query_engine.QueryEngine()
                insert_res = db.query_db(query, commit=True)
                if insert_res["code"] != 200:
                    print(f"Error inserting order {order_id} into SQL Database")
                    print(insert_res["message"])

                bc_date = order.date_created
                # Format Date and Time
                dt_date = utils.parsedate_to_datetime(bc_date)
                date = utc_to_local(dt_date).strftime("%m/%d/%Y")  # ex. 04/24/2024
                time = utc_to_local(dt_date).strftime("%I:%M:%S %p")  # ex. 02:34:24 PM
                products = order.order_products
                product_list = []
                gift_card_only = True
                for x in products:
                    if x["type"] == "physical":
                        gift_card_only = False
                    item = product_engine.Product(x["sku"])
                    product_details = {
                        "sku": item.item_no,
                        "name": item.descr,
                        "qty": x["quantity"],
                        "base_price": x["base_price"],
                        "base_total": x["base_total"],
                    }
                    product_list.append(product_details)

                # Luke, as we work on this project, we need to preserve the printing functionality that
                # the team has come to rely on. We will need to refactor this code to work with the new
                # order class that you have created.

                # If an order contains only gift cards, we will skip printing a ticket unless it is marked
                # 'Pickup in-store'.
                #
                # If an order contains a mix of gift cards and physical products, we will print a ticket for
                # the entire order (GC and products).

                # Currently, the printing does not work correctly with bigcommerce line discounts. We will need
                # to address this issue as well.

                # FILTER OUT GIFT CARDS (NO PHYSICAL PRODUCTS)
                if not gift_card_only:
                    # Create Barcode
                    barcode_filename = "barcode"
                    print("Creating barcode", file=log_file)
                    try:
                        barcode_engine.generate_barcode(
                            data=order_id, filename=barcode_filename
                        )
                    except Exception as err:
                        error_type = "barcode"
                        print(f"Error ({error_type}): {err}", file=log_file)
                    else:
                        print(
                            f"Creating barcode - Success at {datetime.now():%H:%M:%S}",
                            file=log_file,
                        )

                    print("Creating Word Document", file=log_file)
                    # Create the Word document
                    try:
                        doc = DocxTemplate("./templates/ticket_template.docx")
                        barcode = InlineImage(
                            doc, f"./{barcode_filename}.png", height=Mm(15)
                        )  # width in mm
                        context = {
                            # Company Details
                            "company_name": creds.company_name,
                            "co_address": creds.company_address,
                            "co_phone": creds.company_phone,
                            # Order Details
                            "order_number": order_id,
                            "order_date": date,
                            "order_time": time,
                            "order_subtotal": float(order.subtotal_inc_tax),
                            "order_shipping": float(order.shipping_cost_inc_tax),
                            "order_total": float(order.total_inc_tax),
                            # Customer Billing
                            "cb_name": order.billing_first_name
                            + " "
                            + order.billing_last_name,
                            "cb_phone": order.billing_phone,
                            "cb_email": order.billing_email,
                            "cb_street": order.billing_street_address,
                            "cb_city": order.billing_city,
                            "cb_state": order.billing_state,
                            "cb_zip": order.billing_zip,
                            # Customer Shipping
                            "shipping_method": order.shipping_method,
                            "cs_name": order.shipping_first_name
                            + " "
                            + order.shipping_last_name,
                            "cs_phone": order.shipping_phone,
                            "cs_email": order.shipping_email,
                            "cs_street": order.shipping_street_address,
                            "cs_city": order.shipping_city,
                            "cs_state": order.shipping_state,
                            "cs_zip": order.shipping_zip,
                            # Product Details
                            "number_of_items": order.items_total,
                            "ticket_notes": order.customer_message,
                            "products": product_list,
                            "coupon_code": order.order_coupons["code"],
                            "coupon_discount": float(order.coupon_discount),
                            "loyalty": float(order.store_credit_amount),
                            "gc_amount": float(order.gift_certificate_amount),
                            "barcode": barcode,
                        }

                        doc.render(context)
                        ticket_name = f"ticket_{order_id}_{datetime.now().strftime("%m_%d_%y_%H_%M_%S")}.docx"
                        file_path = creds.ticket_location + ticket_name
                        doc.save(file_path)
                    except Exception as err:
                        error_type = "Word Document"
                        print(f"Error ({error_type}): {err}", file=log_file)
                    else:
                        print(
                            f"Creating Word Document - Success at {datetime.now():%H:%M:%S}",
                            file=log_file,
                        )
                        try:
                            # Print the file to default printer
                            os.startfile(file_path, "print")
                        except Exception as err:
                            error_type = "Printing"
                            print(f"Error ({error_type}): {err}", file=log_file)
                        else:
                            print(
                                f"Printing - Success at {datetime.now():%H:%M:%S}",
                                file=log_file,
                            )

                        print(f"Deleting barcode files", file=log_file)

                        # Delete barcode files
                        try:
                            os.remove(f"./{barcode_filename}.png")
                            os.remove(f"./{order_id}.svg")
                        except Exception as err:
                            error_type = "Deleting Barcode"
                            print(f"Error ({error_type}): {err}", file=log_file)
                        else:
                            print(
                                f"Deleting Barcode - Success at {datetime.now():%H:%M:%S}",
                                file=log_file,
                            )
                # Gift Card Only
                else:
                    print(f"Skipping Order #{order_id}: Gift Card Only", file=log_file)
            # Declined Payments
            elif order.status_id == 4:
                print(
                    f"Skipping Order #{order_id}: Order Refunded. Status: {order.payment_status}",
                    file=log_file,
                )
            elif order.status_id == 6:
                print(
                    f"Skipping Order #{order_id}: Payment Declined. Status: {order.payment_status}",
                    file=log_file,
                )
            else:
                print(
                    f"Skipping Order #{order_id}: Payment Status: {order.payment_status}",
                    file=log_file,
                )

            bc_order = BCOrder(order_id)
            bc_order.process()

        except Exception as err:
            error_type = "General Catch"
            print(f"Error ({error_type}): {err}", file=log_file)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        finally:
            print(f"Processing Finished at {datetime.now():%H:%M:%S}\n", file=log_file)
            log_file.close()

    def start_consuming(self):
        while True:
            try:
                self.connect()
                self.channel.basic_consume(
                    queue=self.queue_name, on_message_callback=self.callback
                )
                print("Waiting for messages. To exit press CTRL+C")
                self.channel.start_consuming()
            except KeyboardInterrupt:
                sys.exit(0)
            except pika.exceptions.AMQPConnectionError:
                print("Connection lost. Reconnecting...", file=creds.order_error_log)
                sleep(5)  # Wait before attempting reconnection
            except Exception as err:
                print(err, file=creds.order_error_log)
                sleep(5)  # Wait before attempting reconnection


if __name__ == "__main__":  #
    consumer = RabbitMQConsumer(queue_name="bc_orders")
    consumer.start_consuming()
