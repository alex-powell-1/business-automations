from setup import creds
from setup.date_presets import Dates
import random


class SMSMessages:
    greetings = ['Hey ', 'Hey there ', 'Hi ', 'Greetings ']
    farewells = [
        'See you soon! ',
        'Take care! ',
        'Bye for now! ',
        'See you at the nursery! ',
        'Happy planting! ',
        f'\n-Beth @ {creds.Company.name}',
        f'\n-Brandon @ {creds.Company.name}',
        f'\n-Michelle @ {creds.Company.name}',
    ]

    def __init__(self, dates: Dates):
        self.dates = dates
        self.ftc = self.FirstTimeCustomers(dates)
        self.rc = self.ReturningCustomers(dates)
        self.birthday = self.Birthday(dates)
        self.wholesale = self.Wholesale(dates)

    class FirstTimeCustomers:
        def __init__(self, dates: Dates):
            self.dates = dates
            # Message 1
            # Day after initial purchase
            self.ftc_1_descr = 'First-Time Customer: Message 1'
            self.ftc_1_body = (
                f'Thanks for shopping with {creds.Company.name}. It was a pleasure serving you. '
                f'Please let us know if you have any other questions, or how we can assist you further. '
            )

            # Message 2
            # Three Days after initial purchase
            self.ftc_2_descr = 'First-Time Customer: Message 2 Coupon'
            self.ftc_2_body = (
                f"Don't forget! You've got a {creds.Company.name} coupon for $5 off your next $30 purchase. "
                f"Here's your coupon! It expires on {self.dates.coupon_expiration_day_3:%x} "
            )
            self.ftc_2_image = creds.Coupon.five_off

            # Message 3
            # Seven Days after initial purchase
            self.ftc_3_descr = 'First-Time Customer: Message 3 Review'
            self.ftc_3_body = (
                'Thanks for shopping with us last week! We hope your experience was more than satisfactory. '
                f"If you haven't had a chance, please drop us a review here: {creds.Company.review_link} "
            )

    class ReturningCustomers:
        def __init__(self, dates: Dates):
            self.dates = dates
            # Message 1
            # 1 Day after last purchase - Sends SMS message with brief thank you
            self.rc_1_descr = 'Returning Customer Message 1: Thank you'
            self.rc_1_body = (
                f'Thanks for shopping with {creds.Company.name} yesterday! '
                'Please let us know if you have any questions about your purchase. '
            )

            # Message 2
            # 3 Days after initial purchase - sends MMS Message with coupon and custom expiration date based on last purchase date.
            self.rc_2_descr = 'Returning Customer Message 2: MMS Coupon'
            self.rc_2_body = (
                f"Don't forget! You've got a {creds.Company.name} coupon for $5 off your next $30 purchase. "
                f'It expires on {self.dates.coupon_expiration_day_3:%x}. '
            )
            self.rc_2_image = creds.Coupon.five_off

            # Message 3
            # 7 Day after most recent purchase - Random choice asking for Google Review
            self.rc_3_descr = 'Returning Customer Message 3: Google Review'
            self.rc_3_choice_1 = (
                f'Thanks for shopping with {creds.Company.name} last week! '
                'We hope your experience was more than satisfactory. '
                f"If you haven't had a chance, please drop us a review here: {creds.Company.review_link} "
            )

            self.rc_3_choice_2 = (
                'Thanks for shopping with us last week! '
                f'We hope you had a great experience at {creds.Company.name}. '
                f"If you haven't had a chance, please leave us a review here: {creds.Company.review_link} "
            )

            self.rc_3_body = random.choice([self.rc_3_choice_1, self.rc_3_choice_2])

            # Message 4
            # Day 14, target higher frequency customer_tools
            self.bring_a_friend_coupon = (
                "Here's a coupon for you and a friend! $10 for you and $10 for them when you both spend $50. "
            )

            # Message 5
            # Brandon wants this to end on a Saturday somehow potentially.
            # Day 21, coupon

    class Birthday:
        def __init__(self, dates: Dates):
            self.dates = dates
            self.coupon_1 = (
                f"It's your birth month!! Here's a coupon just for you! Present at checkout for $10 off "
                f'your purchase of $50 or more! '
                f'Your coupon expires on {self.dates.birthday_coupon_expiration_day:%x}. We hope you have an amazing year! '
            )

    class Wholesale:
        def __init__(self, dates: Dates):
            self.dates = dates
            # Message 1 - 1 Day after last purchase - Random choice
            self.message_1_descr = 'Wholesale Customer - Message 1'

            self.wholesale_message_1_1 = (
                'Thanks for being a valued customer. We hope your purchase was more than satisfactory. '
                f'Let us know how we can better serve you. '
            )

            self.wholesale_message_1_2 = (
                'Thank you for your continued support. We really value your business. '
                f"If there's anything we can do to better serve you, "
                f'please let us know! '
            )

            self.wholesale_message_1_3 = (
                "It's been a pleasure serving you. Feel free to let us know if there's anything "
                f'we should carry to better assist you. '
            )

            self.wholesale_message_1_4 = (
                'Thank you so much for choosing us for your project needs. '
                f'We sincerely appreciate each time you come. '
            )

            self.wholesale_message_1_5 = (
                "It's clients like you that keep us going. " f'We really appreciate your continued support. '
            )

            self.wholesale_message_1_bank = [
                self.wholesale_message_1_1,
                self.wholesale_message_1_2,
                self.wholesale_message_1_3,
                self.wholesale_message_1_4,
                self.wholesale_message_1_5,
            ]

            self.message_1 = random.choice(self.wholesale_message_1_bank)


# # MESSAGE 5
# # Brandon wants this to end on a Saturday somehow potentially.
# # Day 21, coupon
# no_friend_coupon = "Don't let this one slip away! Here's a coupon for $10 off $100. "

# # -------- #
# # Message 6
# # -------- #
# # Sixty Days Since Last Sale
# last_sale_sixty_days = (
#     "Don't be a stranger! "
#     "We're adding new products every week for enhancing your home and landscape. "
#     "Here's a coupon on us to help you with your upcoming projects! See you soon! "
# )
# # -------- #
# # Message 7
# # -------- #
# # Six Months Since Last Sale
# last_sale_six_months = 'We miss you! Come see us for 15% off one item plus earn rewards on any purchase over $20. '

# # -------- #
# # Message 8
# # -------- #
# # Twelve Months Since Last Sale
# last_sale_twelve_months = (
#     'We miss you! Come see us for 20% off one item plus earn rewards on any purchase over $20. '
# )
