import os
import requests
import pandas as pd



def download_tiles(csv_path):

    # identifiers should be loaded as strings
    data = pd.read_csv(csv_path, dtype={'identifier': str})
    # Calculate number of tiles (assuming square tiles of 256 pixels)
    data['num_tiles_x'] = (data['width'] / 256).apply(lambda x: int(x) if x.is_integer() else int(x) + 1)

    # Base URL
    base_url = 'http://www.blueprintnhpatlas.org/imageservice/imagepyramid/info///external/nhp/prod23/'

    # Output directory
    for sub, sub_data in data.groupby('sub'):
        output_dir = f'tiles/{sub}'

        os.makedirs(output_dir, exist_ok=True)
        # Loop through each entry to download tiles
        for _, row in sub_data.iterrows():
            identifier = row['identifier']
            zoom = row['tier_count']-1
            num_tiles_x = row['num_tiles_x']
            
            # Conservatively assume square images for num_tiles_y
            num_tiles_y = num_tiles_x

            # Directory for each image
            image_dir = os.path.join(output_dir, str(identifier))

            height = row['height']
            width = row['width']
            siTop = row['siTop']
            siLeft = row['siLeft']
            #http://www.blueprintnhpatlas.org/imageservice/imagepyramid/info///external/nhp/prod23/0536072053/0536072053.aff/TileGroup5/5-5-3.jpg?siTop=2512&siLeft=3824&siWidth=12192&siHeight=8944
            #http://www.blueprintnhpatlas.org/imageservice/imagepyramid/info///external/nhp/prod23/0536046921/0536046921.aff/TileGroup5/8-73-45.jpg?siTop=1152&siLeft=1664&siWidth=37600&siHeight=22752

            print(f"Downloading tiles for ID {identifier} at zoom {zoom}")
            os.makedirs(image_dir, exist_ok=True)

            from joblib import Parallel, delayed

            def download_tile(x, y):
                tile_url = f"{base_url}{identifier}/{identifier}.aff/TileGroup5/{zoom}-{x}-{y}.jpg?siTop={siTop}&siLeft={siLeft}&siWidth={width}&siHeight={height}"
                tile_path = os.path.join(image_dir, f"{zoom}-{x}-{y}.jpg")
                print(f"Tile URL: {tile_url}")

                # Skip download if tile already exists
                if os.path.exists(tile_path):
                    return

                # Request tile
                response = requests.get(tile_url)

                # Save tile if request successful
                if response.status_code == 200:
                    with open(tile_path, 'wb') as f:
                        f.write(response.content)
                else:
                    print(f"Tile not found: {tile_url}")

                # add a delay to avoid overwhelming the server
                #time.sleep(0.1)

            # Use Parallel and delayed for parallel downloads
            Parallel(n_jobs=2)(
                delayed(download_tile)(x, y) for x in range(num_tiles_x) for y in range(num_tiles_y)
            )
        

    print("All tiles downloaded.")


# Load the data
csv_path = 'identifiers/sub-714645-48mo_identifiers.csv'
download_tiles(csv_path)