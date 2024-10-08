# Business Automations

#### Author: Alex Powell
#### Contributor: Luke Barrier

Business Automations is a data integration solution created to enhance productivity, increase revenue, and reduce manual data manipulation at a retail store location using Shopify and NCR Counterpoint Point of Sale. Written in python, the following files should be run as services: server.py, integrator.py, scheduled_tasks.py, async_consumers.py, inventory_sync.py. NSSM.exe can be used to easily install these files as services.

# Overview

## Server

The server (`server.py`), built in flask and served with waitress, handles incoming requests, and sends incoming orders, marketing leads, draft orders to multi-threaded RabbitMQ async queue consumers (`async_consumers.py`). For organization, routes are defined in `routes` as blueprints that are imported in `server.py`.


## Scheduled Tasks

Scheduled tasks (`scheduled_tasks.py`) runs as a windows service (installed with nssm.exe). This script creates a ScheduledTask object that fulfills hourly tasks, daily tasks, sms automations, health checks, backups, and other tasks as required. This script can be run as a scheduled task or as a windows service with the -l argument to run in a loop. For tunneling, a ngrok server is used. 

## Integrator

The integration folder contains modules related to product, category tree (collection), order, customer, newsletter subscriber, promotion, and discount code integration with Shopify using GraphQL queries and mutations. The BigCommerce folder exists from a prior integration and contains much of the logic of the Shopify integration and uses the REST API. This script can be run as a scheduled task or as a windows service with the -l argument to run in a loop. If running in a loop, the Integrator will run during the day at the interval specified by `creds.Integrator.int_day_run_interval` or at night by `creds.Integrator.int_day_run_interval`. Every x syncs, the `SortOrderEngine.sort()` will be called to reorder all products within the collections based on preorder status, sale status, and forecasted revenue by historical reporting.

### Catalog Integration Notes
#### Custom Fields
A number of custom fields are required to facilitate this integration. Binding ID, is_parent, is_preorder, is_in_store_only, preorder_release_dt, preorder_message, is_new, is_back_in_stock, and a number of other custom fields for metafield sync will need to be created. Custom fields for products have been added to the IM_ITEM table. Custom fields for customers have been added to the AR_CUST table. Middleware columns for metafield ID's associated with customer, product, and variant metafields are of type bigint. If adding columns to the AR_CUST or IM_ITEM table, you must run refresh_views.sql to avoid errors at point of sale.

#### Processing Flow
The Catalog integration will first check for updated or deleted media in the `Catalog.process_media()` function. A sync queue is then built by querying the database for items that have been updated since the last sync datetime. After that `Catalog.sync()` will run. Syncing the catalog is broken up into two main objectives: sync the collections, and sync the products. `Catalog.sync()` will first process changes to the NCR Counterpoint category tree (e-commerce categories) by deleting collections and then recursively building and analyzing the tree for updates. Collection additions and updates are sent to Shopify and synced to the database. Next, the main menu, which is a separate organizational unit from the collections on shopify, will be processed similarly. After that `process_product_deletions()` will be called to compare products in the middleware to products in counterpoint and delete products that have been deleted in counterpoint. Finally, the `sync()` function will process products with a last maintained date that is more recent than the last_sync date. This process will use the number of workers specificed in `creds.Integrator.maxworkers` to process each item in the queue. Single items will be processed first and bound products (Shopify product with variants) will be processed at the end of each sync. Shopify API error handling is provided in `shopify_api.py` by the Query class. If a product fails to process, it will be deleted and recreated at the end of the catalog sync. This helps to catch unexpected errors, fix broken mappings, and keep viable products online.

#### Process Media
`Catalog.process_media()` calls two helper functions: `process_images()`, `process_videos()`. `process_images()` will get a list of current product images and their file sizes from the ItemImages folder that is stored locally on the server. It will compare this against a list of images and files sizes from the middleware images table. If items exist in the middleware but do not exist locally, this is a deletion target. Deletion targets will be dealt with by first checking if the image is 'coming-soon.jpg' (a default image in case of missing images). If a default image is being used, the script will check if there are any new photos associated with it. If it does it will delete the default image for the product. Else, it will pass over deletion. If the deletion target image is not the default image, it will delete the image from shopify with a GraphQL mutation and deleting the row from the middeleware images table. In order to maintain the image sort order on Shopify, any remaining product images for the affected product will have their sort order decremented. After deletions are dealt with, this list and any photo addition targets will have their timestamps updated so that `get_sync_queue()` will find these items for processing. `process_videos()` will process videos in a similar manner by creating two lists of videos to compare. Objects existing in the middleware but not in counterpoint will be deleted.

#### Binding ID and Parent Status
In lieu of having a NCR Counterpoint Advanced Pricing license, this product integration relies on using a binding ID field to bind products together into a Shopify product with multiple variants. One product that shares the binding ID must be marked as the parent. 

#### Process() function
`Catalog.Product.Process()` is where all GraphQL mutations are called and products are synced with the middleware. This function first checks if this is an inventory only sync. If so, it bypasses most of the processing and just runs `Shopify.Inventory.update()`. If not, it determines if this is a product update or creation by determining if the product object has a middleware id. If it does, it will run `update()`, otherwise it will run `create()`. 

#### create()
create() begins by constructing a product payload that will be used in a GraphQL mutation. This includes basic product information, product and variant metafields (for properties like mature size, color, bloom season, preorder status, new status, and more), media (images and videos), brand, types, tags, collections, meta title, meta description, html descriptions, and product ID. This payload is then used to create a base item. Various ids are then parsed from the reponse in `get_product_meta_ids()`. In Shopify, items have a default variant and default option that is hidden. The id is retained for deletion later if this is a bound product. If this is a single item, then the single (hidden) variant is then updated with correct sku, location id, buffered inventory, shipping weight, sale price, and some other basic properties. If it is a bound product, a bound product payload will be generated, a bulk set of variants will be created, and then the default variant will be deleted. All remaining variants will then be ordered based on their retail price in ascending order. `time.sleep()` is called for 3 seconds to all all the variant images to process before creating variant images for each variant. Variant meta ids are parsed from response, and finally, the product is published on the predetermined sales channels (Online store, Shop, POS, Google, and Inbox).

#### update()
Update performs in a similar fashion as create but provides IDs for all shopify objects. It also reorders the media which could change at any point that the local file system has photos added or renamed. 

### Inventory Sync Notes
inventory_sync.py is a specialized version of the integrator.py script. It can run very frequently and it will query the database for chnages to the inventory since the last inventory_sync timestamp. If there is a change, it will run the single updateInventory mutation. Every x cycles, this file also generates a csv of inventory for use on a website datatables.net widget.

## Error Handling

Logging and Error handling are handled by the error_handler classes in `setup/error_handler.py`.

# Scheduled Tasks Detailed:

## Twice Per Hour Tasks

### 1) Create New Customers Accounts

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

### Network Health Check
Performs health check on server and notifies admin if server is unreachable

### Reassess Tiered Pricing
Calculate sales totals for wholesale accounts and puts wholesale customers into appropriate tiers


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

### 25) Message 1

Date / Time of Day: 6:30 PM day after first purchase

Message Detail: Thank You (ftc_1_body in first_time_customers.py within sms.sms_messages)

Media: None

### 27) Message 2

Date / Time of Day: 7 PM 3rd day after first purchase

Message Detail: Coupon (ftc_2_body in first_time_customers.py within sms.sms_messages)

Coupon includes expiration date dynamically set to two weeks from last purchase

Media: Image on WebDAV (5 off coupon)

### 28) Message 3

Date / Time of Day: 11:30 AM 7th day after first purchase

Message Detail: Google Review (ftc_3_body in first_time_customers.py within sms.sms_messages)

Media: None

## Returning Customer Campaigns

### 29) Message 1

Date / Time of Day: 11:30 AM day after last purchase

Message Detail: Thank you (rc_1_body in returning_customers.py within sms.sms_messages)

Media: None

### 30) Message 2

Date / Time of Day: 7 PM day after last purchase

Message Detail: Coupon (rc_2_body in returning_customers.py within sms.sms_messages)

Coupon includes expiration date dynamically set to two weeks from last purchase

Media: Image on WebDAV (5 off coupon)

### 31) Message 3

Date / Time of Day: 3:30 PM 7th day after first purchase

Message Detail: Google Review (rc_3_body in returning_customers.py within sms.sms_messages)

Media: None

## Birthday Text Message Campaign

### 32) Message 1

Date / Time of Day: 9 AM on the first date of the month

Message Detail: Message (in birthdays.py within sms.sms_messages) includes a dynamically produced coupon expiration
date.

This date is currently set to be the 10th date of the next month.

Media: Photo stored on the WebDAV server.

## Wholesale Customer Campaign

### 33) Message 1

Date / Time of Day: 10:30 AM day after last purchase

Message Detail: Random Choice Thank You (message_1 in wholesale_sms_messages.py within sms.sms_messages)

Media: None
