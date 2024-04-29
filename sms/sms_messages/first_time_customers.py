from setup.creds import company_name, review_link, five_off_coupon
from setup.date_presets import *

# First_Time_Customers (ftc)

# Message 1
# Day after initial purchase
ftc_1_descr = "First-Time Customer: Message 1"
ftc_1_body = (f"Thanks for shopping with {company_name}. It was a pleasure serving you. "
              f"Please let us know if you have any other questions, or how we can assist you further. ")

# Message 2
# Three Days after initial purchase
ftc_2_descr = "First-Time Customer: Message 2 Coupon"
ftc_2_body = (f"Don't forget! You've got a {company_name} coupon for $5 off your next $30 purchase. "
              f"Here's your coupon! It expires on {coupon_expiration_day_3:%x} ")
ftc_2_image = five_off_coupon

# Message 3
# Seven Days after initial purchase
ftc_3_descr = "First-Time Customer: Message 3 Review"
ftc_3_body = ("Thanks for shopping with us last week! We hope your experience was more than satisfactory. "
              f"If you haven't had a chance, please drop us a review here: {review_link} ")
