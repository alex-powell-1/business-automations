from setup.creds import company_name
import random

# Wholesale Customers

# -------- #
# Message 1
# -------- #

# 1 Day after last purchase
# Random choice
message_1_descr = "Wholesale Customer - Message 1"

wholesale_message_1_1 = ("Thanks for being a valued customer. We hope your purchase was more than satisfactory. "
                         f"Let us know how we can better serve you. -Team {company_name}")

wholesale_message_1_2 = ("Thank you for your continued support. We really value your business. "
                         f"If there's anything we can do to better serve you, "
                         f"please let us know! -Beth @ {company_name}")

wholesale_message_1_3 = ("It's been a pleasure serving you. Feel free to let us know if there's anything "
                         f"we should carry to better assist you. - Beth @ {company_name}")

wholesale_message_1_4 = ("Thank you so much for choosing us for your project needs. "
                         f"We sincerely appreciate each time you come. - Michelle @ {company_name}")

wholesale_message_1_5 = ("It's clients like you that keep us going. "
                         f"We really appreciate your continued support. -Brandon @ {company_name}")

wholesale_message_1_bank = [wholesale_message_1_1, wholesale_message_1_2, wholesale_message_1_3,
                            wholesale_message_1_4, wholesale_message_1_5]

message_1 = random.choice(wholesale_message_1_bank)