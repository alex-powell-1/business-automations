import os
from datetime import datetime

from PIL import Image, ImageOps

from setup import creds

BIGCOMMERCE_SIZE = (1280, 1280)
LANDSCAPE_DESIGN_SIZE = (2560, 2560)
EXIF_ORIENTATION = 0x0112

photo_path = creds.photo_path


def resize_photos(path, log_file, mode="big"):
    print(f"Resize Photos: Starting at {datetime.now():%H:%M:%S}", file=log_file)
    resized_photo_count = 0
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
                    print(f"Found large file {item}. Attempting to resize.", file=log_file)
                    im = Image.open(f"{path}/{item}")
                    im.thumbnail(size, Image.LANCZOS)
                    code = im.getexif().get(EXIF_ORIENTATION, 1)
                    if code and code != 1:
                        im = ImageOps.exif_transpose(im)
                    im.save(f"{path}/{item}", 'JPEG', quality=q)
                    print(f"{item} resized.", file=log_file)
                    resized_photo_count += 1

            # Remove Alpha Layer and Convert PNG to JPG
            if item.lower().endswith("png"):
                print(f"Found PNG file: {item}. Attempting to reformat.", file=log_file)
                im = Image.open(f"{path}/{item}")
                im.thumbnail(size, Image.LANCZOS)
                # Preserve Rotational Data
                code = im.getexif().get(EXIF_ORIENTATION, 1)
                if code and code != 1:
                    im = ImageOps.exif_transpose(im)
                print(f"Stripping Alpha Layer.", file=log_file)
                rgb_im = im.convert('RGB')
                print(f"Saving new file in JPG format.", file=log_file)
                rgb_im.save(f"{path}/{item[:-4]}.jpg", 'JPEG', quality=q)
                im.close()
                print(f"Removing old PNG file", file=log_file)
                os.remove(f'{path}/{item}')
                print(f"Complete", file=log_file)
                resized_photo_count += 1

            # replace .JPEG with .JPG
            if item.lower().endswith("jpeg"):
                print(f"Found file ending with .JPEG", file=log_file)
                im = Image.open(f"{path}/{item}")
                im.thumbnail(size, Image.LANCZOS)
                # Preserve Rotational Data
                code = im.getexif().get(EXIF_ORIENTATION, 1)
                if code and code != 1:
                    im = ImageOps.exif_transpose(im)
                print(f"Saving new file in JPG format.", file=log_file)
                im.save(f"{path}/{item[:-5]}.jpg", 'JPEG', quality=q)
                im.close()
                print(f"Removing old JPEG file", file=log_file)
                os.remove(f'{path}/{item}')
                print(f"Complete", file=log_file)
                resized_photo_count += 1
    if resized_photo_count == 0:
        print("No photos resized/reformatted", file=log_file)
    else:
        print(f"{resized_photo_count} photos resized/reformatted", file=log_file)
    print(f"Resizing Photos: Finished at {datetime.now():%H:%M:%S}", file=log_file)
    print("-----------------------", file=log_file)
