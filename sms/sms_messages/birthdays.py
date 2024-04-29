from setup.creds import birthday_coupon
from setup.date_presets import birthday_coupon_expiration_day

# Birthday Coupon Text

BIRTHDAY_COUPON = birthday_coupon

birthday_coupon_1 = (f"It's your birth month!! Here's a coupon just for you! Present at checkout for $10 off "
                     f"your purchase of $50 or more! "
                     f"Your coupon expires on {birthday_coupon_expiration_day:%x}. We hope you have an amazing year! ")
