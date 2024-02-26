Business Automations

Author: Alex Powell

Business Automations is a collection of automations built to enhance productivity and reduce manual data manipulation at a retail store location 
that integrates a SQL database with the BigCommerce e-commerce platform. Automations include:

- Automatic Current Stock Upload
- Automatic SMS/MMS notifications based on customer buying behavior
- Automatic photo resizing and reformatting for e-commerce platform
- Automatic setting of bestseller status, sort order status, and featured items status based on predictive analysis
- Automatic setting of e-commerce related items based on popular choices (by revenue) within same categories
- Automatic setting of tierred pricing levels for commercial accounts based on past-six-months revenue
- Automatic setting of e-commerce flags for products based on stock quantities and statuses
- Automatic setting of product brands based on product descriptions
- Automatic generation and email delivery of styled "Administrative Report" and "Revenue Report" to administrative and accounting departments


At the core of this application, there are four "engines" (database engine, sms engine, WebDav engine, and email engine) for getting, setting, and distributing data.
These modules are found within the setup folder.

This application also has useful classes for products (in product_tools.products.py) and customers (in customers.customers.py) that instantiate useful variables
in the constructor from the SQL database. 

Logging of all data manipulation is handled by functions with the setup.create_log.py module.
