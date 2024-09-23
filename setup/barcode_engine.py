import code128
from setup.utilities import generate_random_code
from setup import creds


def generate_barcode(
    data=None, filename=None, length=None, separator=None, segment_length=None, path=creds.Company.barcodes
) -> str:
    if length:
        data = generate_random_code(length)
        if separator and segment_length:
            # add separator to the code every segment_length characters (e.g. EHGIDD -> EH-GI-DD)
            data = separator.join(data[i : i + segment_length] for i in range(0, len(data), segment_length))
    if not filename:
        filename = data

    """Generates a Code 128 barcode (svg) and converts to png for use in templates"""
    filename = f'{path}/{filename}.png'
    try:
        code128.image(data).save(filename)  # with PIL present
    except Exception as e:
        print(e)
    else:
        return filename


def generate_svg_barcode(data, filename):
    with open(f'{filename}.svg', 'w') as f:
        f.write(code128.svg(data))


if __name__ == '__main__':
    print(generate_barcode('12345'))
