class CustomerWebhook:
    def __init__(self, data):
        self.email = data['email']
        self.id = data['id']
        self.first_name = data['first_name']
        self.last_name = data['last_name']
        self.phone = data['phone']
        self.addresses = data['addresses']
        if 'state' in data['email_marketing_consent']:
            self.email_consent = True if data['email_marketing_consent']['state'] == 'subscribed' else False
            self.email_consent_updated_at = data['email_marketing_consent']['consent_updated_at']
        else:
            self.email_consent = False
            self.email_consent_updated_at = None

        if self.phone and 'state' in data['sms_marketing_consent']:
            self.sms_consent = True if data['sms_marketing_consent']['state'] == 'subscribed' else False
            self.sms_consent_updated_at = data['sms_marketing_consent']['consent_updated_at']
        else:
            self.sms_consent = False
            self.sms_consent_updated_at = None
        self.created_at = data['created_at']
        self.updated_at = data['updated_at']
        self.last_order_id = data['last_order_id']
        self.note = data['note']

    def __str__(self) -> str:
        result = 'Customer Webhook\n'
        result += '----------------\n'
        result += f'Customer ID: {self.id}\n'
        result += f'Email: {self.email}\n'
        result += f'First Name: {self.first_name}\n'
        result += f'Last Name: {self.last_name}\n'
        result += f'Phone: {self.phone}\n'
        result += f'Email Consent: {self.email_consent}\n'
        result += f'Email Consent Updated At: {self.email_consent_updated_at}\n'
        result += f'SMS Consent: {self.sms_consent}\n'
        result += f'SMS Consent Updated At: {self.sms_consent_updated_at}\n'
        result += f'Created At: {self.created_at}\n'
        result += f'Updated At: {self.updated_at}\n'
        result += f'Last Order ID: {self.last_order_id}\n'
        result += f'Note: {self.note}\n'
        return result
