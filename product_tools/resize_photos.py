import os
from setup import creds
from PIL import Image, ImageOps

BIGCOMMERCE_SIZE = (1280, 1280)
LANDSCAPE_DESIGN_SIZE = (2560, 2560)
EXIF_ORIENTATION = 0x0112

photo_path = creds.photo_path


def resize_photos(path, mode="big"):
    list_of_files = os.listdir(path)
    if mode == "big":
        size = BIGCOMMERCE_SIZE
        q = 90
    else:
        size = LANDSCAPE_DESIGN_SIZE
        q = 100

    for item in list_of_files:
        try:
            file_size = os.path.getsize(f"{path}/{item}")
        except FileNotFoundError:
            continue
        else:
            if item.lower().endswith("jpg"):
                # Resize files larger than 1.9 MB
                if file_size > 1800000:
                    im = Image.open(f"{path}/{item}")
                    im.thumbnail(size, Image.LANCZOS)
                    code = im.getexif().get(EXIF_ORIENTATION, 1)
                    if code and code != 1:
                        im = ImageOps.exif_transpose(im)
                    im.save(f"{path}/{item}", 'JPEG', quality=q)

            # Remove Alpha Layer and Convert PNG to JPG
            if item.lower().endswith("png"):
                im = Image.open(f"{path}/{item}")
                im.thumbnail(size, Image.LANCZOS)
                code = im.getexif().get(EXIF_ORIENTATION, 1)
                if code and code != 1:
                    im = ImageOps.exif_transpose(im)
                rgb_im = im.convert('RGB')
                rgb_im.save(f"{path}/{item[:-4]}.jpg", 'JPEG', quality=q)
                im.close()
                os.remove(f'{path}/{item}')

            # replace .JPEG with .JPG
            if item.lower().endswith("jpeg"):
                im = Image.open(f"{path}/{item}")
                im.thumbnail(size, Image.LANCZOS)
                code = im.getexif().get(EXIF_ORIENTATION, 1)
                if code and code != 1:
                    im = ImageOps.exif_transpose(im)
                im.save(f"{path}/{item[:-5]}.jpg", 'JPEG', quality=q)
                im.close()
                os.remove(f'{path}/{item}')
