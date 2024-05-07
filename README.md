# Business Automations

Author: Alex Powell

Business Automations is a collection of automations built to enhance productivity and reduce manual data manipulation at 
a retail store location that integrates a SQL database with the BigCommerce e-commerce platform. Automations include:

## Engines
At the core of this application, there are four "engines" (database engine, sms engine, WebDav engine, and email engine) 
for getting, setting, and distributing data. These modules are found within the setup folder.

This application has useful classes for products (in product_tools.products.py) and customers (in customers.customers.py) 
that instantiate useful variables in the constructor from the SQL database.

## Logging 
When the application runs for the first time each day, it will create a log file:
log_directory/business_automations/automations_month_day_year.txt
This log file will be passed into each of the automation functions as an argument and error and success messages will
be written to the file before finally closing after all functions have concluded.

## Decoupling of Processes 
If a process fails for any reason, the error will be logged and the next process will continue. Total time per process
is logged within each section, and total number of errors will be shown at the conclusion of the log.

# Automations:
## Twice Per Hour Tasks
### 1) Create New Counterpoint Customers
Parse data from a csv of marketing leads. Find leads from today and check if they are already
counterpoint customers. If they are not, create a new customer in counterpoint with the info supplied
by the customer. Utilizes the NCR Counterpoint API. Keeps a record of newly created contacts in 
the log_directory/new_customers.csv

### 2) Set 'Contact 1' field in Counterpoint to be the concatenation of FST_NAM and LST_NAM
Per the request of company admin, Counterpoint's Contact 1 field should be the concatenation of FST_NAM and LST_NAM
where the customer name type is 'P' (Person). The process begins by finding customers with a null in CONTCT_1 and filters
out any customers whose first name is 'Change' and last name is 'Name'. These disregarded edge cases are the result of 
employee error when inputting new customers at the point of sale. This function then title cases the result and updates
the SQL database with the new value for CONTCT_1

## Hourly Tasks
### 3) Check for internet connection
This server will check for an internet connection. If the function determines there is no connection, the application
will log the disconnected state and force a restart. To determine if there is an internet connection, the server will
attempt to ping three reliable hosts ("https://www.google.com/", "1.1.1.1", "8.8.8.8"). If ping fails to ALL three, the 
application will declare an disconnected state and initiate the reboot.

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
in January of 2024, though the process will maintain the aspect ratios of non-1:1 ratio images. Furthermore, this process
will maintain the image rotation metadata (EXIF_ORIENTATION).
#### Reformatting
All files ending in .jpeg will be renamed to .jpg.
All files ending in .png will have their alpha layer stripped and will be converted to .jpg

## Every Other Hour Tasks
Tasks performed on even hours between 6 AM and 8 PM
### 6) Set Products to 'Inactive' Status
This task will find all products with a STAT of 'A' (active) whose IM_INV quantity available has fallen below 1 (this
includes negative quantity) and sets the status to 'V' (inactive) in SQL. This process will also update the last 
maintained date to the current time. The process makes us of the Product class to check for a successful update. Since 
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
product, it will generate a list of all the child products that belong to the binding ID, find out the total sold for each
of these items and add them to a running sum that is then updated to the parent product.
##### Considerations:
This is a time-intensive task (O(n) API calls) that is currently scheduled to run once daily during the night (non-business hours)

### 11) Update Related Items
This process updates related items on the BigCommerce in the following ways:
First, it examines a dictionary of recommended items for each product category. It will assign these to each product. 
It will then check this time period last year for this product category and determine which products were popular. It will
also assign these popular items to the item as related items.
##### Considerations:
This is a time-intensive task (O(n) API calls) that is currently scheduled to run once daily during the night (non-business hours)