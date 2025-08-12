# I'll parse the XML to extract the unique identifiers using Python.
import xml.etree.ElementTree as ET
import os
import glob
import pandas as pd



def extract_unique_identifiers(xml_path, id_dir):

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
            path = section_image.find("path")

            print(structure_id.text, structure_id.text is not None);
            
            # Store the identifier and its associated fields
            unique_ids[identifier] = {
                "resolution": float(resolution.text) if resolution is not None else None,
                "sample": int(section_number.text) if section_number is not None else None,
                "sub": str(specimen_id).replace('sub-',''),
                "structure_id": int(structure_id.text) if structure_id.text is not None else None,
                "tier_count": int(tier_count.text) if tier_count.text is not None else None,
                "width": int(width.text) if width.text is not None else None,
                "height": int(height.text) if height.text is not None else None,
                "siLeft": int(siLeft.text) if siLeft.text is not None else None,
                "siTop": int(siTop.text) if siTop.text is not None else None,
                "path": path.text if path is not None else None
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

    return csv_path


def get_identifiers(xml_dir='/media/tfunck/external/dev-nhp-atlas/xml/', id_dir='/media/tfunck/external/dev-nhp-atlas/identifiers'):

    # List all XML files in the directory
    xml_files = glob.glob(os.path.join(xml_dir, '*.xml'))

    if not xml_files:
        print("No XML files found in the specified directory.")
        return

    # Process each XML file
    #for xml_file in xml_files:
    #    extract_unique_identifiers(xml_file, id_dir=id_dir)

    # Load XML files
    # Call the function to extract identifiers
    id_df_list = []
    for xml_path in xml_files:
        id_df_list.append(extract_unique_identifiers(xml_path, id_dir=id_dir))

    # Combine all DataFrames into one
    #combined_df = pd.concat(id_df_list, ignore_index=True)

    # Save the combined DataFrame to a single CSV file
    #combined_csv_path = '/media/tfunck/external/dev-nhp-atlas/identifiers/combined_identifiers.csv'
    #combined_df.to_csv(combined_csv_path, index=False)
    #print(f"Combined data saved to {combined_csv_path}")
    
    return id_df_list