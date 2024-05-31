import code128


def generate_barcode(data, filename):
    """Generates a Code 128 barcode (svg) and converts to png for use in templates"""
    code128.image(data).save(f"{filename}.png")  # with PIL present


def generate_svg_barcode(data, filename):
    with open(f"{filename}.svg", "w") as f:
        f.write(code128.svg(data))
