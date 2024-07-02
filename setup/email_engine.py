from setup import creds
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication

from email.utils import formataddr
from jinja2 import Template


class Email:
	def __init__(
		self,
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
		self.from_name = from_name
		self.from_address = from_address
		self.from_pw = from_pw
		self.recipients_list = recipients_list
		self.subject = subject
		self.content = content
		self.mode = mode
		self.product_photo = product_photo
		self.barcode = barcode
		self.logo = logo
		self.attachment = attachment
		self.staff = staff
		self.send_email()

	def create_email(self):
		self.msg = MIMEMultipart(self.mode)
		self.msg['From'] = formataddr((self.from_name, self.from_address))
		self.msg['To'] = formataddr((self.to_name, self.to_address))
		self.msg['Subject'] = self.subject

		self.msg_html = MIMEText(self.content, 'html')
		self.msg.attach(self.msg_html)

		if self.logo:
			with open(creds.logo, 'rb') as logo_file:
				logo = logo_file.read()
				msg_logo = MIMEImage(logo, 'jpg')
				msg_logo.add_header('Content-ID', '<image1>')
				msg_logo.add_header('Content-Disposition', 'inline', filename='Logo.jpg')
				self.msg.attach(msg_logo)

		if self.product_photo is not None:
			with open(self.product_photo, 'rb') as item_photo:
				product = item_photo.read()
				msg_product_photo = MIMEImage(product, 'jpg')
				msg_product_photo.add_header('Content-ID', '<image2>')
				msg_product_photo.add_header('Content-Disposition', 'inline', filename='product.jpg')
				self.msg.attach(msg_product_photo)

		if self.barcode is not None:
			with open(self.barcode, 'rb') as item_photo:
				product = item_photo.read()
				msg_barcode = MIMEImage(product, 'png')
				msg_barcode.add_header('Content-ID', '<image3>')
				msg_barcode.add_header('Content-Disposition', 'inline', filename='barcode.png')
				self.msg.attach(msg_barcode)

		if self.attachment:
			with open(creds.design_pdf_attachment, 'rb') as file:
				pdf = file.read()
				attached_file = MIMEApplication(_data=pdf, _subtype='pdf')
				attached_file.add_header(
					_name='content-disposition', _value='attachment', filename=f'{creds.design_pdf_name}'
				)
				self.msg.attach(attached_file)

	def send_email(self):
		def send():
			with smtplib.SMTP('smtp.gmail.com', port=587) as connection:
				connection.ehlo()
				connection.starttls()
				connection.ehlo()
				connection.login(user=self.from_address, password=self.from_pw)
				connection.sendmail(self.from_address, self.to_address, self.msg.as_string().encode('utf-8'))
				connection.quit()

		if self.staff:
			# Dictionary of recipients in creds config
			for person in self.recipients_list:
				self.to_name = creds.staff[person]['full_name']
				self.to_address = creds.staff[person]['email']
				self.create_email()
				send()
		else:
			# General Use
			for k, v in self.recipients_list.items():
				self.to_name = k
				self.to_address = v
				self.create_email()
				send()


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

	Email(
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
