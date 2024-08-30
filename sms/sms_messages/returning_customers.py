from setup.creds import company_name, review_link, five_off_coupon, company_name
from setup.date_presets import coupon_expiration_day_3
import random

# SMS/MMS Messages for Returning Customers

# MESSAGE 1
# 1 Day after last purchase - Sends SMS message with brief thank you
rc_1_descr = 'Returning Customer Message 1: Thank you'
rc_1_body = (
    f'Thanks for shopping with {company_name} yesterday! '
    'Please let us know if you have any questions about your purchase. '
)

# MESSAGE 2
# 3 Days after initial purchase - sends MMS Message with coupon and custom expiration date based on last purchase date.
rc_2_descr = 'Returning Customer Message 2: MMS Coupon'
rc_2_body = (
    f"Don't forget! You've got a {company_name} coupon for $5 off your next $30 purchase. "
    f'It expires on {coupon_expiration_day_3:%x}. '
)
rc_2_image = five_off_coupon

# MESSAGE 3
# 7 Day after most recent purchase - Random choice asking for Google Review
rc_3_descr = 'Returning Customer Message 3: Google Review'
rc_3_choice_1 = (
    f'Thanks for shopping with {company_name} last week! '
    'We hope your experience was more than satisfactory. '
    f"If you haven't had a chance, please drop us a review here: {review_link} "
)

rc_3_choice_2 = (
    'Thanks for shopping with us last week! '
    f'We hope you had a great experience at {company_name}. '
    f"If you haven't had a chance, please leave us a review here: {review_link} "
)

# rc_3_choice_3 = (f"It was good seeing you last week! We hope you had a great time at {company_name}. "
#                  f"If you haven't had a chance, please leave us a review here: {review_link} ")

rc_3_body = random.choice([rc_3_choice_1, rc_3_choice_2])

# MESSAGE 4
# Day 14, target higher frequency customer_tools
bring_a_friend_coupon = (
    "Here's a coupon for you and a friend! $10 for you and $10 for them when you both spend $50. "
)

# MESSAGE 5
# Brandon wants this to end on a Saturday somehow potentially.
# Day 21, coupon
no_friend_coupon = "Don't let this one slip away! Here's a coupon for $10 off $100. "

# -------- #
# Message 6
# -------- #
# Sixty Days Since Last Sale
last_sale_sixty_days = (
    "Don't be a stranger! "
    "We're adding new products every week for enhancing your home and landscape. "
    "Here's a coupon on us to help you with your upcoming projects! See you soon! "
)
# -------- #
# Message 7
# -------- #
# Six Months Since Last Sale
last_sale_six_months = 'We miss you! Come see us for 15% off one item plus earn rewards on any purchase over $20. '

# -------- #
# Message 8
# -------- #
# Twelve Months Since Last Sale
last_sale_twelve_months = (
    'We miss you! Come see us for 20% off one item plus earn rewards on any purchase over $20. '
)
