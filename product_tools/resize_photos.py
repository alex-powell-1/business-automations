import os
from datetime import datetime

from PIL import Image, ImageOps

from setup import creds
from setup.error_handler import ScheduledTasksErrorHandler as error_handler

BIGCOMMERCE_SIZE = (1280, 1280)
LANDSCAPE_DESIGN_SIZE = (2560, 2560)
EXIF_ORIENTATION = 0x0112

photo_path = creds.photo_path


def resize_photos(path, mode='big'):
	error_handler.logger.info(f'Resize Photos: Starting at {datetime.now():%H:%M:%S}')
	resized_photo_count = 0
	list_of_files = os.listdir(path)
	if mode == 'big':
		size = BIGCOMMERCE_SIZE
		q = 90
	else:
		size = LANDSCAPE_DESIGN_SIZE
		q = 100

	for item in list_of_files:
		try:
			file_size = os.path.getsize(f'{path}/{item}')
		except FileNotFoundError:
			continue
		else:
			if item.lower().endswith('jpg'):
				# Resize files larger than 1.9 MB
				if file_size > 1800000:
					error_handler.logger.info(f'Found large file {item}. Attempting to resize.')
					im = Image.open(f'{path}/{item}')
					im.thumbnail(size, Image.LANCZOS)
					code = im.getexif().get(EXIF_ORIENTATION, 1)
					if code and code != 1:
						im = ImageOps.exif_transpose(im)
					im.save(f'{path}/{item}', 'JPEG', quality=q)
					error_handler.logger.info(f'{item} resized.')
					resized_photo_count += 1

			# Remove Alpha Layer and Convert PNG to JPG
			if item.lower().endswith('png'):
				error_handler.logger.info(f'Found PNG file: {item}. Attempting to reformat.')
				im = Image.open(f'{path}/{item}')
				im.thumbnail(size, Image.LANCZOS)
				# Preserve Rotational Data
				code = im.getexif().get(EXIF_ORIENTATION, 1)
				if code and code != 1:
					im = ImageOps.exif_transpose(im)
				error_handler.logger.info('Stripping Alpha Layer.')
				rgb_im = im.convert('RGB')
				error_handler.logger.info('Saving new file in JPG format.')
				rgb_im.save(f'{path}/{item[:-4]}.jpg', 'JPEG', quality=q)
				im.close()
				error_handler.logger.info('Removing old PNG file')
				os.remove(f'{path}/{item}')
				error_handler.logger.info('Complete')
				resized_photo_count += 1

			# replace .JPEG with .JPG
			if item.lower().endswith('jpeg'):
				error_handler.logger.info('Found file ending with .JPEG')
				im = Image.open(f'{path}/{item}')
				im.thumbnail(size, Image.LANCZOS)
				# Preserve Rotational Data
				code = im.getexif().get(EXIF_ORIENTATION, 1)
				if code and code != 1:
					im = ImageOps.exif_transpose(im)
				error_handler.logger.info('Saving new file in JPG format.')
				im.save(f'{path}/{item[:-5]}.jpg', 'JPEG', quality=q)
				im.close()
				error_handler.logger.info('Removing old JPEG file')
				os.remove(f'{path}/{item}')
				error_handler.logger.info('Complete')
				resized_photo_count += 1
	if resized_photo_count == 0:
		error_handler.logger.info('No photos resized/reformatted')
	else:
		error_handler.logger.info(f'{resized_photo_count} photos resized/reformatted')
	error_handler.logger.info(f'Resizing Photos: Finished at {datetime.now():%H:%M:%S}')
