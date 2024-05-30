import code128


def generate_barcode(data, filename):
    """Generates a Code 128 barcode (svg) and converts to png for use in templates"""
    code128.image(data).save(f"{filename}.png")  # with PIL present
    with open(f"{data}.svg", "w") as f:
        f.write(code128.svg(data))

#barcode_engine.generate_barcode(data=order_id, filename=barcode_filename)