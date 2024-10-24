import subprocess
import os

def generate_hls_files(video_path: str, output_path: str):
    """Generate HLS files for video streaming."""
    if not os.path.exists(video_path):
        raise FileNotFoundError('Video file not found')
    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))
    
    command = [
        'ffmpeg',
        '-i', video_path,
        '-codec', 'copy',
        '-start_number', '0',
        '-hls_time', '5',
        '-hls_list_size', '0',
        '-f', 'hls',
        output_path
    ]

    subprocess.run(command, check=True)

if __name__ == '__main__':
    src_video = r'.\static\videos\landscaping.mp4'
    dest_path = r'static\videos\hls\landscaping\landscaping.m3u8'
    generate_hls_files(src_video, dest_path)