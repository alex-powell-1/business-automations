from integration.products import *

if __name__ == '__main__':
    integrator = Integrator(date_presets.business_start_date)
    customers = integrator.Customers(last_sync=date_presets.business_start_date)
    customers.sync()