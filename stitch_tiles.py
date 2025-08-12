import os
import PIL
import pandas as pd


import imageio.v3 as iio
from joblib import Parallel, delayed
import numpy as np

def process_column(x, max_y, tile_dir, tile_size, zoom_level):
    offset_y = 0
    
    stitched_column = np.zeros( [(max_y + 1) * tile_size, tile_size], dtype=np.uint8)
    
    for y in range(max_y + 1):

        # Find the corresponding tile file
        tile_file = f"{tile_dir}/{zoom_level}-{x}-{y}.jpg" # Check if the tile file exists

        tile_dim_y = tile_size
        tile_dim_x = tile_size
        
        if os.path.exists(tile_file) :
            
            # load tile path with imageio
            try :
                tile_image = np.mean( iio.imread(tile_file), axis=2)
                
                tile_dim_y = tile_image.shape[0]
                tile_dim_x = tile_image.shape[1]
            except Exception as e:
                os.remove(tile_file)  # Remove the corrupted tile
                print(f"Error loading tile {tile_file}: {e}")
                continue

            
            stitched_column[offset_y:(offset_y + tile_dim_y), :tile_dim_x] = tile_image
        
        offset_y += tile_dim_y

    return stitched_column, x 

# Loop through each image identifier
def stitch_image(identifier, zoom_level, tile_dir, stitched_image_path):
    """    Stitch tiles for a given identifier at a specific zoom level."""

    tile_files = [f for f in os.listdir(tile_dir) if f.startswith(f"{zoom_level}-") and f.endswith('.jpg')]

    if not tile_files : 
        print(f"No tiles found for identifier {identifier} at zoom level {zoom_level} in\n\t {tile_dir}")
        return None

    # Extract tile positions
    tile_coords = [(int(f.split('-')[1]), int(f.split('-')[2].split('.')[0])) for f in tile_files]
    max_y = max(y for y, _ in tile_coords)
    max_x = max(x for _, x in tile_coords)

    # Assume tiles are 256x256 pixels
    tile_size = 256

    print(f"\t\tStitching image for ID {identifier}, dimensions: {max_x + 1}x{max_y + 1}")
    # Create a new blank image

    n_items = len(tile_files)


    # Create a numpy array for the stitched image
    stitched_array = np.zeros(
        ((max_y + 1) * tile_size, (max_x + 1) * tile_size), dtype=np.uint8
    )
    
    offset_x = 0

    results = Parallel(n_jobs=-1)(delayed(process_column)(x, max_y, tile_dir, tile_size, zoom_level) for x in range(max_x + 1))

    for stitched_column, x in results:
        stitched_array[:, x * tile_size:(x + 1) * tile_size] = stitched_column

        

    print("\t\tStitched image saved at:", stitched_image_path)
    # Save the stitched image
    iio.imwrite(stitched_image_path, stitched_array, plugin='pillow')


def stitch_all_images(csv_path, input_tiles_dir, output_image_dir, clobber:bool=False):
    """
    Stitch all images based on the identifiers in the CSV file.
    """
    print('Stitching all images...')

    # identifiers should be loaded as strings
    data = pd.read_csv(csv_path, dtype={'identifier': str})
    
    df_list = []
    csv_list = []
    
    # Output directory
    for sub, sub_data in data.groupby('sub'):
        sub_dir = f'{output_image_dir}/sub-{sub}'
        
        sub_out_csv = sub_dir + '_stitched_images.csv'
        
        csv_list.append(sub_out_csv)
        
        if os.path.exists(sub_out_csv) and not clobber:
            print(f"Stitched images for subject {sub} already exist at {sub_out_csv}. Skipping stitching.")
            continue

        os.makedirs(sub_dir, exist_ok=True)
        
        print(f"\tStitching images for subject: {sub}")

        sub_data['raw_jpg'] = sub_data['sample'].apply(lambda x: f'{sub_dir}/sub-{sub}_hemi-B_acq-nissl_y-{x}.jpg')

        # Loop through all identifiers in tiles directory
        for _, row in sub_data.iterrows():
            identifier = row['identifier']
            zoom_level = row['tier_count'] - 1
            stitched_image_path = row['raw_jpg']
            
            sub_tile_dir = f'{input_tiles_dir}/sub-{sub}/{identifier}/'

            if not os.path.exists(stitched_image_path) :
                
                stitch_image(identifier, zoom_level, sub_tile_dir, stitched_image_path)


        sub_data.to_csv(sub_out_csv, index=False)
        print(f"{sub} = all images stitched.")
        df_list.append(sub_data)
        
        
    out_csv = os.path.join(output_image_dir, 'stitched_images.csv')
    clobber=True
    # concatenate all dataframes
    if not os.path.exists(out_csv) or clobber:
        df = pd.concat([ pd.read_csv(csv) for csv in csv_list ], ignore_index=True)
        print(df['sub'].unique())
        df.to_csv(out_csv, index=False)

    return out_csv
              
