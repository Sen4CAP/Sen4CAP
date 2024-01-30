#!/usr/bin/env python

import argparse

import numpy as np
import rasterio

def interpolate_zeros(input, output):
    value_to_fill = 0

    with rasterio.open(input) as src:
        image = src.read(1)
    # Create a copy of the matrix to avoid modifying the original
    interpolated_matrix = image.copy()
    
    # Define the directions for the 8 neighboring pixels
    window_size = 3
    
    # Continue until nothing changed
    ok = True
    while ok:
        count = np.count_nonzero(interpolated_matrix==value_to_fill)
        print(count)

        ok = False
        zero_positions = np.argwhere(interpolated_matrix == value_to_fill)
        
        for x, y in zero_positions:
            x_min = max(x - window_size // 2, 0)
            x_max = min (x_min + window_size, interpolated_matrix.shape[0])
            y_min = max(y - window_size // 2, 0)
            y_max = min(y_min + window_size, interpolated_matrix.shape[1])
            window = interpolated_matrix[x_min:x_max, y_min:y_max]
            window_valid_values = window[window != value_to_fill]                
            
            # if window_valid_values.size > 0 and replacement != value_to_fill:
            if window_valid_values.size > 0:
                # mean
                # replacement = round(np.mean(window_valid_values))
                
                # median
                # replacement = round(np.median(window_valid_values))

                # mode
                vals, counts = np.unique(window_valid_values, return_counts=True)
                index = np.argmax(counts)
                replacement = vals[index]

                interpolated_matrix[x, y] = replacement
                ok = True
                
    # Save the interpolated image
    with rasterio.open(output, 'w', driver='GTiff', height=image.shape[0], 
                       width=image.shape[1], count=1, dtype=image.dtype, 
                       crs=src.crs, transform=src.transform) as dst:
        dst.write(interpolated_matrix, 1)

    print("Interpolation complete")
    
def main():
    parser = argparse.ArgumentParser(
        description="Broceliande results post processing (interpolate zeroes, etc.)"
    )
    parser.add_argument("-i", "--input", help="Input Broceliande classes raster")
    parser.add_argument("-o", "--output", help="Output postprocess Broceliande classes raster")
    
    args = parser.parse_args()

    interpolate_zeros(args.input, args.output)
    
if __name__ == "__main__":
    main()



