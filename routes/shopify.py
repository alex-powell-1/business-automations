from database import Database
from integration.shopify_api import Shopify
from integration.shopify_customers import Customers
from flask import request, jsonify, Blueprint
from setup.creds import API
import pika
from setup.utilities import verify_webhook, tb, convert_utc_to_local
from setup.error_handler import ProcessInErrorHandler, Logger, OutOfStockErrorHandler
from setup import creds
import json
from shop.models.webhooks import CustomerWebhook
from routes.limiter import limiter


class EventID:
    """Shopify Event ID class to prevent duplicate processing of webhooks."""

    draft_update = 0
    draft_create = 0
    order_create = 0
    customer_create = 0
    customer_update = 0
    product_update = 0
    variant_out_of_stock = 0
    collection_update = 0


shopify_routes = Blueprint('shopify_routes', __name__, template_folder='routes')

default_rate = creds.API.default_rate


@shopify_routes.route(API.Route.Shopify.order_create, methods=['POST'])
@limiter.limit(default_rate)
def orders():
    """Webhook route for incoming orders. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers

    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.order_create:
        return jsonify({'success': True}), 200
    EventID.order_create = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    if 'refund_line_items' in webhook_data:
        webhook_data['id'] = webhook_data['order_id']

    with open('order_create.json', 'a') as f:
        json.dump(webhook_data, f)

    order_id = webhook_data['id']

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue=creds.Consumer.orders, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=creds.Consumer.orders,
            body=str(order_id),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        connection.close()
    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(
            error=f'Error sending order {order_id} to RabbitMQ: {e}',
            origin=API.Route.Shopify.order_create,
            traceback=tb(),
        )

    return jsonify({'success': True}), 200


@shopify_routes.route(API.Route.Shopify.draft_create, methods=['POST'])
@limiter.limit(default_rate)
def draft_create():
    """Webhook route for newly created draft orders. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers

    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.draft_create:
        return jsonify({'success': True}), 200
    EventID.draft_create = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    print('DRAFT ORDER RECEIVED.')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401
    order_id = webhook_data['id']

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue=creds.Consumer.draft_create, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=creds.Consumer.draft_create,
            body=str(order_id),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        connection.close()
    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(
            error=f'Error sending order {order_id} to RabbitMQ: {e}',
            origin=API.Route.Shopify.draft_create,
            traceback=tb(),
        )

    return jsonify({'success': True}), 200


@shopify_routes.route(API.Route.Shopify.draft_update, methods=['POST'])
@limiter.limit(default_rate)
def draft_update():
    """Webhook route for updated draft orders. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers

    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.draft_update:
        return jsonify({'success': True}), 200
    EventID.draft_update = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401
    order_id = webhook_data['id']

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue=creds.Consumer.draft_update, durable=True)

        channel.basic_publish(
            exchange='',
            routing_key=creds.Consumer.draft_update,
            body=str(order_id),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )
        connection.close()
    except Exception as e:
        ProcessInErrorHandler.error_handler.add_error_v(
            error=f'Error sending order {order_id} to RabbitMQ: {e}',
            origin=API.Route.Shopify.draft_update,
            traceback=tb(),
        )

    return jsonify({'success': True}), 200


@shopify_routes.route(API.Route.Shopify.customer_create, methods=['POST'])
@limiter.limit(default_rate)
def customer_create():
    """Webhook route for updated customers. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers

    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.customer_create:
        return jsonify({'success': True}), 200
    EventID.customer_create = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')

    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    id = webhook_data['id']

    error_handler = ProcessInErrorHandler.error_handler
    logger = error_handler.logger

    logger.info(f'Processing Customer Create: {id}')

    if Customers.Customer.has_metafield(cust_id=id, key='number'):
        logger.info(f'Customer {id} has customer number metafield. Skipping.')
    else:
        try:
            street = None
            city = None
            state = None
            zip_code = None
            phone = webhook_data['phone']
            email = webhook_data['email']

            addrs = webhook_data['addresses']

            if len(addrs) > 0:
                a = addrs[0]
                street = a['address1']
                city = a['city']
                state = a['province']
                zip_code = a['zip']

                # Merge new customer into existing customer
                # Delete new customer from Shopify

        except Exception as e:
            error_handler.add_error_v(
                error=f'Error adding customer {id}: {e}', origin=API.Route.Shopify.customer_create, traceback=tb()
            )
            return jsonify({'error': 'Error adding customer'}), 500
        else:
            logger.success(f'Customer {id} added successfully.')
            return jsonify({'success': True}), 200

    logger.success(f'Customer Create Finished: {id}')
    return jsonify({'success': True}), 200


@shopify_routes.route(API.Route.Shopify.customer_update, methods=['POST'])
@limiter.limit(default_rate)
def customer_update():
    """Webhook route for updated customers. Sends to RabbitMQ queue for asynchronous processing"""
    headers = request.headers
    webhook_data = request.json
    # print(webhook_data)
    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.customer_update:
        return jsonify({'success': True}), 200
    EventID.customer_update = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    customer = CustomerWebhook(webhook_data)
    Logger(creds.Logs.webhooks_customer_update).log(f'Webhook: Customer Update, ID: {customer.id}')

    return jsonify({'success': True}), 200


@shopify_routes.route(API.Route.Shopify.product_update, methods=['POST'])
@limiter.limit(default_rate)
def product_update():
    """Webhook route for updated products. Sends to RabbitMQ queue for asynchronous processing"""
    webhook_data = request.json
    headers = request.headers
    logger = Logger(creds.Logs.webhooks_product_update)
    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.product_update:
        return jsonify({'success': True}), 200
    EventID.product_update = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    # Get product data
    product_id = webhook_data['id']
    title = webhook_data['title']
    description = webhook_data['body_html']
    status = webhook_data['status']
    tags = webhook_data['tags']
    item_no = Database.Shopify.Product.get_parent_item_no(product_id)

    logger.log(f'Webhook: Product Update, SKU:{item_no}, Product ID: {product_id}, Web Title: {title}')

    if item_no and description:
        # Update product description in Counterpoint - Skip timestamp update (avoid loop)
        Database.Counterpoint.Product.HTMLDescription.update(
            item_no=item_no, html_descr=description, update_timestamp=False, eh=ProcessInErrorHandler
        )

    # Get SEO data
    seo_data = Shopify.Product.SEO.get(product_id)
    if seo_data:
        meta_title = seo_data['title']
        meta_description = seo_data['description']
    else:
        meta_title = None
        meta_description = None

    # Get product Metafields
    metafields = Shopify.Product.Metafield.get(product_id)
    features = None
    botanical_name = None
    plant_type = None
    light_requirements = None
    size = None
    features = None
    bloom_season = None
    bloom_color = None
    color = None
    is_featured = None
    in_store_only = None
    is_preorder_item = None
    preorder_message = None
    preorder_release_date = None

    for i in metafields['product_specifications']:
        if i['key'] == 'botanical_name':
            botanical_name = i['value']

        if i['key'] == 'plant_type':
            plant_type = i['value']

        if i['key'] == 'light_requirements':
            light_requirements = i['value']

        if i['key'] == 'size':
            size = i['value']

        if i['key'] == 'features':
            features = i['value']

        if i['key'] == 'bloom_season':
            bloom_season = i['value']

        if i['key'] == 'bloom_color':
            bloom_color = i['value']

        if i['key'] == 'color':
            color = i['value']

    for i in metafields['product_status']:
        if i['key'] == 'featured':
            is_featured = True if i['value'] == 'true' else False

        if i['key'] == 'in_store_only':
            in_store_only = True if i['value'] == 'true' else False

        if i['key'] == 'preorder_item':
            is_preorder_item = True if i['value'] == 'true' else False

        if i['key'] == 'preorder_message':
            preorder_message = i['value']

        if i['key'] == 'preorder_release_date':
            preorder_release_date = convert_utc_to_local(i['value'])

    # Get media data
    media_payload = []
    media = webhook_data['media']

    if media:
        for m in media:
            id = m['id']
            position = m['position']
            alt_text = m['alt']
            if alt_text and position < 4:  # First 4 images only at this time.
                media_payload.append({'position': position, 'id': id, 'alt_text': alt_text})

    if item_no:
        update_payload = {'product_id': product_id, 'item_no': item_no}

        if status:
            update_payload['status'] = status
        if title:
            update_payload['title'] = title
        if meta_title:
            update_payload['meta_title'] = meta_title
        if meta_description:
            update_payload['meta_description'] = meta_description

        if tags:
            update_payload['tags'] = tags

        if botanical_name:
            update_payload['botanical_name'] = botanical_name
        if plant_type:
            update_payload['plant_type'] = plant_type
        if light_requirements:
            update_payload['light_requirements'] = light_requirements
        if size:
            update_payload['size'] = size
        if features:
            update_payload['features'] = features
        if bloom_season:
            update_payload['bloom_season'] = bloom_season
        if bloom_color:
            update_payload['bloom_color'] = bloom_color
        if color:
            update_payload['color'] = color
        if is_featured:
            update_payload['is_featured'] = is_featured
        if in_store_only:
            update_payload['in_store_only'] = in_store_only
        if is_preorder_item:
            update_payload['is_preorder_item'] = is_preorder_item
        if preorder_message:
            update_payload['preorder_message'] = preorder_message
        if preorder_release_date:
            update_payload['preorder_release_date'] = preorder_release_date

        if media_payload:
            for m in media_payload:
                position = m['position']
                update_payload[f'alt_text_{position}'] = m['alt_text']
        try:
            Database.Counterpoint.Product.update(update_payload)
        except Exception as e:
            ProcessInErrorHandler.error_handler.add_error_v(
                error=f'Error updating product {item_no}: {e}',
                origin=API.Route.Shopify.product_update,
                traceback=tb(),
            )

    return jsonify({'success': True}), 200


@shopify_routes.route(API.Route.Shopify.collection_update, methods=['POST'])
@limiter.limit(default_rate)
def collection_update():
    """Webhook route for collection update"""

    webhook_data = request.json
    headers = request.headers
    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.collection_update:
        return jsonify({'success': True}), 200
    EventID.collection_update = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    ProcessInErrorHandler.logger.info(f'Collection Update: {webhook_data['title']}')

    return jsonify({'success': True}), 200


@shopify_routes.route(API.Route.Shopify.variant_out_of_stock, methods=['POST'])
@limiter.limit(default_rate)
def variant_out_of_stock():
    """Webhook route for notification when a product variant is out of stock."""

    webhook_data = request.json
    headers = request.headers
    event_id = headers.get('X-Shopify-Event-Id')
    if event_id == EventID.variant_out_of_stock:
        return jsonify({'success': True}), 200
    EventID.variant_out_of_stock = event_id

    data = request.get_data()
    hmac_header = headers.get('X-Shopify-Hmac-Sha256')
    if not hmac_header:
        return jsonify({'error': 'Unauthorized'}), 401
    verified = verify_webhook(data, hmac_header)
    if not verified:
        return jsonify({'error': 'Unauthorized'}), 401

    OutOfStockErrorHandler.logger.info(f'Variant Out of Stock: {webhook_data}')

    try:
        product_id = int(webhook_data['product_id'])

        shopify_product = Shopify.Product.get(product_id=product_id)['product']
        if shopify_product['totalInventory'] < 1:
            collections = Shopify.Product.get_collection_ids(product_id=product_id)
            for collection in collections:
                Shopify.Collection.move_to_bottom(
                    collection_id=collection, product_id_list=[product_id], eh=OutOfStockErrorHandler
                )
    except Exception as e:
        OutOfStockErrorHandler.error_handler.add_error_v(
            error=f'Error processing variant out of stock: {e}', origin='Variant Out of Stock', traceback=tb()
        )

    return jsonify({'success': True}), 200
