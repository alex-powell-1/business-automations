# Business Automations

#### Author: Alex Powell

Business Automations is a collection of automations built to enhance productivity and reduce manual data manipulation at
a retail store location that integrates a SQL database with the BigCommerce e-commerce platform. Automations include:

## Engines

At the core of this application, there are four "engines" (database engine, sms engine, WebDav engine, and email engine)
for getting, setting, and distributing data. These modules are found within the setup folder.

## Logging

When the application runs for the first time each day, it will create a log file:
log_directory/business_automations/automations_month_day_year.txt
This log file will be passed into each of the automation functions as an argument and error and success messages will
be written to the file before finally closing after all functions have concluded.

## Decoupled Processes

If a process fails for any reason, the error will be logged and the next process will continue.
Total time per process is logged within each section, and total number of errors will be shown at the conclusion of the
log.

# Automations:

## Twice Per Hour Tasks

### 1) Create New Counterpoint Customers

Parse data from a csv of marketing leads. Find leads from today and check if they are already
counterpoint customers. If they are not, create a new customer in counterpoint with the info supplied
by the customer. Utilizes the NCR Counterpoint API. Keeps a record of newly created contacts in
the log_directory/new_customers.csv

### 2) Set 'Contact 1' Field in Counterpoint for Non-Business Entities

Per the request of company admin, Counterpoint's Contact 1 field should be the concatenation of FST_NAM and LST_NAM
where the customer name type is 'P' (Person). The process begins by finding customers with a null in CONTCT_1 and
filters
out any customers whose first name is 'Change' and last name is 'Name'. These disregarded edge cases are the result of
employee error when inputting new customers at the point of sale. This function then title cases the result and updates
the SQL database with the new value for CONTCT_1

## Hourly Tasks

### 3) Check for Internet Connection

This server will check for an internet connection. If the function determines there is no connection, the application
will log the disconnected state and force a restart. To determine if there is an internet connection, the server will
attempt to ping three reliable hosts ("https://www.google.com/", "1.1.1.1", "8.8.8.8"). If ping fails to ALL three, the
application will declare a disconnected state and initiate the reboot.

### 4) Inventory Upload

This process will get long description, price, and quantity available for all active items (defined below) for
the retail and wholesale customer segments and upload this data as a csv to the BigCommerce WebDAV server for use in
a datatables.net data table implementation that is hosted at oururl.com/availability and oururl.com/commercial.

#### Definition of active item

An item will be active should it's STAT field be set to 'A' and it's QTY_AVAIL >= 0

### 5) Photo Resizing/Reformatting

This process will resize and reformat photos to the ideal specifications for the BigCommerce e-commerce platform.

#### Resizing

If a .jpg image is larger than 1.8 MB, the function will use the Lanczos resampling algorithm to reduce file size while
also reducing resolution to the specified constant in the module. For BigCommerce, this has been set to 1280x1280 pixels
in January 2024, though the process will maintain the aspect ratios of non-1:1 ratio images. Furthermore, this process
will maintain the image rotation metadata (EXIF_ORIENTATION).

#### Reformatting

All files ending in .jpeg will be renamed to .jpg.
All files ending in .png will have their alpha layer stripped and will be converted to .jpg

## Every Other Hour Tasks

Tasks performed on even hours between 6 AM and 8 PM

### 6) Set Products to 'Inactive' Status

This task will find all products with a STAT of 'A' (active) whose IM_INV quantity available has fallen below 1 (this
includes negative quantity) and sets the status to 'V' (inactive) in SQL. This process will also update the last
maintained date to the current time. The process makes use of the Product class to check for a successful update. Since
this change in status may need to be inspected by retail management staff, a separate log of all status changes is
written at log_directory/inactive_products/inactive_products_month_day_year.csv

### 7) Update Product Brands

To assist with proper front-end presentation on the e-commerce site, this task will update the Brand field
(IM_ITEM.PROF_COD_1) of items. For products with vendor specific information, the brand will be updated to the
company name. For products that have a brand in their long description, the task will use a regular expression in
SQL to select items and then update their brand based on values stored in a dictionary in the creds modules. Processed
items will have their last maintained date field updated in SQL so the data integration process will update them on BC.

### 8) Stock Buffer Updates

To prevent overselling items online, a stock buffer will be calculated and applied to items.

#### Rules:

As an overall generic rule, upon creation, items will have a statically set buffer of 5 items. This is done in the data
dictionary. This rule is insufficient for many product categories. As a result, dynamic buffers are set as follows:

1) All products with the brand matching a key in the vendor dictionary will have their buffer set to the matching value.
2) All products with a product category in the buffer bank will have their buffers set dynamically based on item price.

As a current example, a piece of pottery that has a price greater than $100 will have a buffer of 0, while a piece of
pottery that has a price of $20 will have a buffer of 3.

### 9) Email List Export

To assist our email marketing team, a minimal list of customers with corresponding reward point balances is exported for
upload to our email marketing platform. Since our emails most often include reward point balances, this helps us to give
our customers accurate information via campaigns

## Once Per Day Tasks

### 10) Update Total Sold

This process will update BigCommerce (through their API) with the total number of times an item has sold at our point
of sale. Since many of our products are bound into a merged item with multiple sizes, this process is fairly complex and
will figure out if a product is bound. If it is not, then it will calculate the total times it has sold based on all
previous transactions and make the update product API call via the big_commerce.big_products.py module. If it is a bound
product, it will generate a list of all the child products that belong to the binding ID, find out the total sold for
each
of these items and add them to a running sum that is then updated to the parent product.

##### Considerations:

This is a time-intensive task (O(n) API calls) that is currently scheduled to run once daily during the night (
non-business hours)

### 11) Update Related Items

This process updates related items on the BigCommerce in the following ways:
First, it examines a dictionary of recommended items for each product category. It will assign these to each product.
It will then check this time period last year for this product category and determine which products were popular. It
will
also assign these popular items to the item as related items.

##### Considerations:

This is a time-intensive task (O(n) API calls) that is currently scheduled to run once daily during the night (
non-business hours)

### 12) Set 'Always Online' Status

Set top performing products to custom 'Always Online' statis if they are included in the top 200 performing items since
the beginning of the previous year. This marker will be used to preserve these products from being taken offline in the
event that they go out of stock for a time, and we initiate a site-cleaning method to clear out old stock.

### 13) Set 'Sort Order' on E-Commerce Platform

BigCommerce will sort items by their sort order property. This algorithm determines the sort order for all products in
the e-commerce store.

##### Sort Order Ranking

The lower the sort order, the more prominently the item appears in the search results

##### Sort Order Logic

Items will be ranked in order of the revenue they produced during the same 45-day period last year. That is, one year
ago
and 45 days forward from that day. As a seasonal business, this gives us an indication of what products are likely to
be popular during this time. Once an order is determined, the top performing items will be assigned to the inverse
length of all the items (1 may become -820 if there are 820 items). This then iterates through all the items and sets
the
sort order in this manner. Per the request of management, items that are newly in-stock (as determined through a recent
receiving date) will receive a super-inflated sort order positioning of the top score + 8 (which makes it slightly less
popular than the top positions)

##### Considerations:

This is a time-intensive task (O(n) API calls) that is currently scheduled to run once daily during the night (
non-business hours)

### 14) Set 'Featured' Status on E-Commerce Platform

This process finds the top 15 items from this time last year, checks their stock level, and if they have stock available
it will set them to featured for a prominent display on the homepage of the e-commerce site.

##### Considerations:

So far, I have not implemented a method of removing featured status. This is being done manually.

### 15) Administrative Report to Administrative Team

This process prepares relevant business performance metrics into a templated html based email and sends to
the administrative team. The recipients of the administrative team are in the creds module. Currently, this report
does not use strict Jinja templating, but rather the functions output the data within html tags and append the body
section of the html file. Once rendering is done, emails are sent out via the email engine. Gmail credentials must be
maintained within the creds module.

### 16) Revenue Report to Accounting Team

This process is a more specific report generated for the accounting department and delivered on Sunday for ease of
access
on Monday morning. This function will be deprecated in Summer 2024 and replaced with a Jinja template that has more
details about
credit card pay codes (per the request of accounting)

### 17) Customer Followup Email to Sales Team

We receive leads for the landscape design service via web form that communicates with a flask server through a ngrok
tunnel.
These leads are written to a csv in the log_directory/design_request_leads.csv. This function will parse the csv,
determine
which leads came in yesterday (or the morning of), and use a Jinja template (follow_up.html in reporting.templates) to
generate a html email with customer name, phone, project goals, timeline, and address and send this email to our sales
team
for following-up with potential customers.

### 18) Stock Notification Email with Coupon Generation

We receive request for stock notification via a web form that communicates with a flask server through a ngrok tunnel.
These stock notification requests are written to a csv in log_directory/stock_notification_log.csv. This function will
parse the csv, determine if the requested item has come back into stock, and if it has it will render a stock
notification
email template (stock_notification.html in customers.templates). This template produces a customized email with the
recipient's name, their requested item's photo, a description of their item, and a coupon with a randomly generated
coupon
code (freshly generated and sent to the BigCommerce API) with a 3-day expiration for use. When the new coupon is created
the process will write the coupon information to log_directory/coupons/created_coupon_log.csv. Finally, if this process
has
been completed, the stock notification request will be deleted out of the original csv. If the item is still out of
stock,
the line will be left intact. Strictly speaking, the csv file is fully reconstructed each time, but the result
is that of removing the line.

##### Coupon Details

At this time, the automatically generated coupon is for $10 off the item on orders of at least $100.

##### Duplicate Requests

The flask server handles duplicate requests by notifying the user that they have already signed up for notifications for
this
product.

### 19) Delete Expired Coupons from E-Comm Platform

This process will query BigCommerce for all coupons. For each coupon, it will save the expiration date and convert this
date to local time before comparing it with today's date. If it is determined that the coupon has expired, it will log
the details of the coupon in log_directory/coupons/deleted_coupons.csv and then send a DELETE request to the BC API.
This is scheduled to run at night prior to midnight.

### 20) Automated Webscraping for Competitor Pricing Analysis

This process uses the Selenium web driver to automate logging into a competitor's wholesale availability page, scraping
all the data from a html table, parsing it with BeautifulSoup and using Pandas to rendering the resulting dataframe
into a CSV file for further price analysis. The process is written in a generic sense, so it could be applied to more
target sites in the future. Credentials for the website logins is stored in a dictionary for each site in the
credentials module.

### 21) Automated Off-Site Backups

This process iterates through the keys of a dictionary of targeted folders and files that need to be copied to an
off-site storage location for backup purposes. Backup success or error is logged in the log file. Backup locations
are stored in the credentials file.

## Customer Edge Case Handling

### 22) Remove Invalid E-Comm Customers from Text Funnels

It is common to get out-of-state purchases from first-time customers who expect a shipping service. When this happens,
our integration brings down the order to our database and updates the LST_SAL_DAT column of the customer record. When
we initiate a refund from the point of sale, it does not revert the customers LST_SAL_DAT back to an old value because
that information is not stored in the database. This process will find these targeted customers and delete out their
LST_SAL_DAT column data. It will replace LST_SAL_DT with the date from their previous transaction if there is one, but
if there is not, it will set this to null. This will effectively remove these customers from the SMS marketing funnel,
as the text automations rely on these dates to target recipients.

### 23) Remove Wholesale Customers from Loyalty Program

This process removes wholesale customers from loyalty program if sales team member inadvertently adds them to the
program.

### 24) Fix Negative Loyalty Balances

There are some uncommon situations that can lead to a customer having a negative balance for the loyalty program. We
often will text customers their reward balance in our mms/sms campaigns and sending a negative balance is not
acceptable.
To fix this, negative values are set to a balance of 0 each day. NULL values are not allowed in this field.

# SMS/MMS Text Messaging Automations

Text Message Automations will use the create_customer_text() function in the sms_automations module to construct a
personalized message for recipients that are chosen based on targeting rules as defined in a SQL query. This message
construction function has arguments for sending reward balances, sending images, utilizing a test mode, and log
location.

## First Time Customer Campaigns

These automations target customers based on their FST_SAL_DAT column data

### Message 1

Date / Time of Day: 6:30 PM day after first purchase

Message Detail: Thank You (ftc_1_body in first_time_customers.py within sms.sms_messages)

Media: None

### Message 2

Date / Time of Day: 7 PM 3rd day after first purchase

Message Detail: Coupon (ftc_2_body in first_time_customers.py within sms.sms_messages)

Coupon includes expiration date dynamically set to two weeks from last purchase

Media: Image on WebDAV (5 off coupon)

### Message 3

Date / Time of Day: 11:30 AM 7th day after first purchase

Message Detail: Google Review (ftc_3_body in first_time_customers.py within sms.sms_messages)

Media: None

## Returning Customer Campaigns

### Message 1

Date / Time of Day: 11:30 AM day after last purchase

Message Detail: Thank you (rc_1_body in returning_customers.py within sms.sms_messages)

Media: None

### Message 2

Date / Time of Day: 7 PM day after last purchase

Message Detail: Coupon (rc_2_body in returning_customers.py within sms.sms_messages)

Coupon includes expiration date dynamically set to two weeks from last purchase

Media: Image on WebDAV (5 off coupon)

### Message 3

Date / Time of Day: 3:30 PM 7th day after first purchase

Message Detail: Google Review (rc_3_body in returning_customers.py within sms.sms_messages)

Media: None

## Birthday Text Message Campaign

### Message 1

Date / Time of Day: 9 AM on the first date of the month

Message Detail: Message (in birthdays.py within sms.sms_messages) includes a dynamically produced coupon expiration
date.

This date is currently set to be the 10th date of the next month.

Media: Photo stored on the WebDAV server.

## Wholesale Customer Campaign

### Message 1

Date / Time of Day: 10:30 AM day after last purchase

Message Detail: Random Choice Thank You (message_1 in wholesale_sms_messages.py within sms.sms_messages)

Media: None