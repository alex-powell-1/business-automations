from setup import creds
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication

from email.utils import formataddr
from jinja2 import Template


def send_html_email(
	from_name,
	from_address,
	from_pw,
	recipients_list,
	subject,
	content,
	mode,
	product_photo=None,
	barcode=None,
	logo=True,
	attachment=None,
	staff=False,
):
	if staff:
		# Dictionary of recipients in creds config
		for person in recipients_list:
			to_name = creds.staff[person]['full_name']
			to_address = creds.staff[person]['email']
	else:
		# General Use
		for k, v in recipients_list.items():
			to_name = k
			to_address = v

		msg = MIMEMultipart(mode)
		msg['From'] = formataddr((from_name, from_address))
		msg['To'] = formataddr((to_name, to_address))
		msg['Subject'] = subject

		msg_html = MIMEText(content, 'html')
		msg.attach(msg_html)

		if logo:
			with open(creds.logo, 'rb') as logo_file:
				logo = logo_file.read()
				msg_logo = MIMEImage(logo, 'jpg')
				msg_logo.add_header('Content-ID', '<image1>')
				msg_logo.add_header('Content-Disposition', 'inline', filename='Logo.jpg')
				msg.attach(msg_logo)

		if product_photo is not None:
			with open(product_photo, 'rb') as item_photo:
				product = item_photo.read()
				msg_product_photo = MIMEImage(product, 'jpg')
				msg_product_photo.add_header('Content-ID', '<image2>')
				msg_product_photo.add_header(
					'Content-Disposition', 'inline', filename='product.jpg'
				)
				msg.attach(msg_product_photo)

		if barcode is not None:
			with open(barcode, 'rb') as item_photo:
				product = item_photo.read()
				msg_barcode = MIMEImage(product, 'png')
				msg_barcode.add_header('Content-ID', '<image3>')
				msg_barcode.add_header('Content-Disposition', 'inline', filename='barcode.png')
				msg.attach(msg_barcode)

		if attachment:
			with open(creds.design_pdf_attachment, 'rb') as file:
				pdf = file.read()
				attached_file = MIMEApplication(_data=pdf, _subtype='pdf')
				attached_file.add_header(
					_name='content-disposition',
					_value='attachment',
					filename=f'{creds.design_pdf_name}',
				)
				msg.attach(attached_file)

		with smtplib.SMTP('smtp.gmail.com', port=587) as connection:
			connection.ehlo()
			connection.starttls()
			connection.ehlo()
			connection.login(user=from_address, password=from_pw)
			connection.sendmail(from_address, to_address, msg.as_string().encode('utf-8'))
			connection.quit()


def design_email(first_name, email):
	"""Send email and PDF to customer in response to request for design information."""
	recipient = {first_name: email}
	with open('./templates/design_lead/customer_email.html', 'r') as file:
		template_str = file.read()

	jinja_template = Template(template_str)

	email_data = {
		'title': creds.email_subject,
		'greeting': f'Hi {first_name},',
		'service': creds.service,
		'company': creds.company_name,
		'list_items': creds.list_items,
		'signature_name': creds.signature_name,
		'signature_title': creds.signature_title,
		'company_phone': creds.company_phone,
		'company_url': creds.company_url,
		'company_reviews': creds.company_reviews,
	}

	email_content = jinja_template.render(email_data)

	send_html_email(
		from_name=creds.company_name,
		from_address=creds.gmail_user,
		from_pw=creds.gmail_pw,
		recipients_list=recipient,
		subject=creds.email_subject,
		content=email_content,
		mode='mixed',
		logo=False,
		attachment=creds.design_pdf_attachment,
	)
