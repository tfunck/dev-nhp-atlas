import os
from PIL import Image

# Directory containing tiles
base_tiles_dir = 'tiles'

# Output directory
output_dir = 'images'
os.makedirs(output_dir, exist_ok=True)

# Loop through each image identifier
def stitch_image(identifier, zoom_level):
    image_dir = os.path.join(base_tiles_dir, identifier)
    tile_files = [f for f in os.listdir(image_dir) if f.startswith(f"{zoom_level}-") and f.endswith('.jpg')]

    # Extract tile positions
    tile_coords = [(int(f.split('-')[1]), int(f.split('-')[2].split('.')[0])) for f in tile_files]
    max_x = max(x for x, y in tile_coords)
    max_y = max(y for x, y in tile_coords)

    # Assume tiles are 256x256 pixels
    tile_size = 256

    # Create a new blank image
    stitched_image = Image.new('RGB', ((max_x + 1) * tile_size, (max_y + 1) * tile_size))

    # Paste each tile
    for tile_file, (x, y) in zip(tile_files, tile_coords):
        tile_path = os.path.join(image_dir, tile_file)
        tile_image = Image.open(tile_path)
        stitched_image.paste(tile_image, (x * tile_size, y * tile_size))

    # Save stitched image
    stitched_image_path = os.path.join(output_dir, f"{identifier}_stitched.jpg")
    stitched_image.save(stitched_image_path)
    print(f"Saved stitched image: {stitched_image_path}")

# Loop through all identifiers in tiles directory
for identifier in os.listdir(base_tiles_dir):
    zoom_level = max(int(f.split('-')[0]) for f in os.listdir(os.path.join(base_tiles_dir, identifier)))
    stitch_image(identifier, zoom_level)

print("All images stitched.")
