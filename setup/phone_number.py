class PhoneNumber:
    def __init__(self, phone_number):
        self.raw = self.strip_number(phone_number)
        self.area_code = self.get_area_code()
        self.exchange = self.get_exchange()
        self.subscriber_number = self.get_subscriber_number()

    def strip_number(self, phone_number):
        return phone_number.replace('+1', '').replace('-', '').replace('(', '').replace(')', '').replace(' ', '')

    def get_area_code(self):
        return self.raw[0:3]

    def get_exchange(self):
        return self.raw[3:6]

    def get_subscriber_number(self):
        return self.raw[6:]

    def to_cp(self):
        return f'{self.area_code}-{self.exchange}-{self.subscriber_number}'

    def to_twilio(self):
        return f'+1{self.area_code}{self.exchange}{self.subscriber_number}'

    def __str__(self):
        return f'({self.area_code}) {self.exchange}-{self.subscriber_number}'


if __name__ == '__main__':
    print(PhoneNumber('+1 (800) 555-1212'))
    print(PhoneNumber('+1 (800  ) 55  512-12').to_cp())
    print(PhoneNumber('+1 ( 800 55 ) 5-1212').to_twilio())
