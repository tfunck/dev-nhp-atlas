import os
import requests
import pandas as pd
import time

from joblib import Parallel, delayed
from PIL import Image

def test_download(tile_path):
    try:
        img = Image.open(tile_path)
        img.verify()  # Verify that it is a valid image
    except Exception as e:
        print(f"Invalid image file {tile_path}: {e}")
        os.remove(tile_path)
        
def save_tile(tile_path, response):
    # Save tile if request successful
    if response.status_code == 200:
        with open(tile_path, 'wb') as f:
            f.write(response.content)
            
        # test if the file is a valid jpg
        #test_download(tile_path)

    return 0


def download_tile(x, y, base_url, identifier, zoom, siTop, siLeft, width, height, image_dir, ntries = 5):
    #{identifier}/{identifier}.aff
    tile_url = f"{base_url}/TileGroup5/{zoom}-{x}-{y}.jpg?siTop={siTop}&siLeft={siLeft}&siWidth={width}&siHeight={height}"
    tile_path = os.path.join(image_dir, f"{zoom}-{x}-{y}.jpg")

    
    # Skip download if tile already exists
    if os.path.exists(tile_path):
        return
    # Request tile
    for attempt in range(ntries):
        try:
            response = requests.get(tile_url)
            
            save_tile(tile_path, response)
            # check that response content is not empty
            if response.status_code == 200 and response.content:
                break
            elif response.status_code == 404:
                print(f"Tile not found: {tile_url}")



        except requests.exceptions.ConnectionError as e:
            if attempt < ntries - 1:
                print(f"Connection error for {tile_url}, retrying in 60 seconds... (attempt {attempt+1}/{ntries})")
                time.sleep(60)
            else:
                print(f"Failed to download {tile_url} after {ntries} attempts. Exiting.")
                raise
    else:
        print(f"Tile not found: {tile_url}")
            

def download_tiles(csv_path, output_dir, n_jobs:int = 300):

    # identifiers should be loaded as strings
    data = pd.read_csv(csv_path, dtype={'identifier': str})
    
    # Calculate number of tiles (assuming square tiles of 256 pixels)
    data['num_tiles_x'] = (data['width'] / 256).apply(lambda x: int(x) if x.is_integer() else int(x) + 1)

    # Base URL
    base_url = 'http://www.blueprintnhpatlas.org/imageservice/imagepyramid/info//' #/external/nhp/prod23/'

    # Output directory
    for sub, sub_data in data.groupby('sub') :

        sub_output_dir = f'{output_dir}/sub-{sub}'

        print(sub_output_dir); 
        os.makedirs(sub_output_dir, exist_ok=True)

        # Loop through each entry to download tiles
        for i,( _ , row) in enumerate(sub_data.iterrows()):
            identifier = row['identifier']
            zoom = row['tier_count']-1
            num_tiles_x = row['num_tiles_x']

            if i % 20 == 0:
                print(f'Completed: {i}/{len(sub_data)} for sub {sub}')

            identifier_file = f'{sub_output_dir}/{identifier}.txt'

            # Check if identifier file already exists
            if os.path.exists(identifier_file):
                continue
            
            # Conservatively assume square images for num_tiles_y
            num_tiles_y = num_tiles_x

            # Directory for each image
            image_dir = os.path.join(sub_output_dir, str(identifier))

            height = row['height']
            width = row['width']
            siTop = row['siTop']
            siLeft = row['siLeft']
            #http://www.blueprintnhpatlas.org/imageservice/imagepyramid/info///external/nhp/prod23/0536072053/0536072053.aff/TileGroup5/5-5-3.jpg?siTop=2512&siLeft=3824&siWidth=12192&siHeight=8944
            #http://www.blueprintnhpatlas.org/imageservice/imagepyramid/info///external/nhp/prod23/0536046921/0536046921.aff/TileGroup5/8-73-45.jpg?siTop=1152&siLeft=1664&siWidth=37600&siHeight=22752

            path = row['path'] #/external/nhp/prod17/0536070077/0536070077.aff</path>
            
            curr_base_url = base_url + path + '/'

            print(f"Downloading tiles for ID {identifier} at zoom {zoom}")
            os.makedirs(image_dir, exist_ok=True)

            # Use Parallel and delayed for parallel downloads
            Parallel(n_jobs=n_jobs)(
                delayed(download_tile)(x, y, curr_base_url, identifier, zoom, siTop, siLeft, width, height, image_dir) for x in range(num_tiles_x) for y in range(num_tiles_y)
            )
            
            # Create a file to mark that this identifier has been processed
            with open(identifier_file, 'w') as f:
                f.write(f"Processed identifier {identifier}\n")
        

    print("All tiles downloaded.")


