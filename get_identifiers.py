# I'll parse the XML to extract the unique identifiers using Python.
import xml.etree.ElementTree as ET
import os
import glob
import pandas as pd



def extract_unique_identifiers(xml_path, id_dir='identifiers'):

    os.makedirs(id_dir, exist_ok=True)

    specimen_id = str(os.path.basename(xml_path).replace(".xml", ""))

    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Extract unique identifiers from the 'path' element within each 'section-image'
    unique_ids = {}
    for section_image in root.findall(".//section-image"):
        path_element = section_image.find("path")
        if path_element is not None:
            path_text = path_element.text
            # Extract the numeric identifier from the path
            identifier = path_text.split("/")[-1].replace(".aff", "")
            
            # Extract additional fields
            resolution = section_image.find("resolution")
            section_number = section_image.find("section-number")
            structure_id = section_image.find("structure-id")
            tier_count = section_image.find("tier-count")
            width = section_image.find("width")
            height = section_image.find("height")
            siLeft = section_image.find("x")
            siTop = section_image.find("y")
            
            # Store the identifier and its associated fields
            unique_ids[identifier] = {
                "resolution": float(resolution.text) if resolution is not None else None,
                "sample": int(section_number.text) if section_number is not None else None,
                "sub": str(specimen_id),
                "structure_id": int(structure_id.text) if structure_id is not None else None,
                "tier_count": int(tier_count.text) if tier_count is not None else None,
                "width": int(width.text) if width is not None else None,
                "height": int(height.text) if height is not None else None,
                "siLeft": int(siLeft.text) if siLeft is not None else None,
                "siTop": int(siTop.text) if siTop is not None else None
            }



    # DataFrame
    import pandas as pd
    df = pd.DataFrame.from_dict(unique_ids, orient='index') 
    df.index.name = 'identifier'
    df.reset_index(inplace=True)

    # Display DataFrame
    print(df.head())

    # Save DataFrame to CSV
    csv_path = f"{id_dir}/{specimen_id}_identifiers.csv"
    df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}")

    # Display extracted identifiers
    unique_ids_list = sorted(list(unique_ids))
    print("Unique Identifiers:", len(unique_ids_list))

    return df


# Load XML file
xml_path = 'xml/sub-714645_48mo.xml'
# Call the function to extract identifiers
id_df_list = []
for xml_path in glob.glob('xml/*.xml'):
    id_df_list.append(extract_unique_identifiers(xml_path))

# Combine all DataFrames into one
combined_df = pd.concat(id_df_list, ignore_index=True)