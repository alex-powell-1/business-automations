from database import Database as db
from datetime import datetime as dt
from setup.error_handler import LeadFormErrorHandler, ProcessInErrorHandler
import os
import time
from setup import utilities
from docxtpl import DocxTemplate

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


if __name__ == '__main__':
    Printer.print_order(5687251632295)
    # generate_barcode(data='as', filename='barcode2')
