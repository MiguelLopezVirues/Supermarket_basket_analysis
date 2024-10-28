
# data visualization imports
import matplotlib.pyplot as plt 
import matplotlib.dates as mdates
import seaborn as sns

def plot_line_labels(ax, interval=1, contrast=False):
    if not isinstance(contrast, bool):
        raise TypeError(f"Expected 'contrast' to be of type 'bool', but got {type(contrast).__name__} instead.")
    
    for line in ax.lines:
        line_color = line.get_color()  # Get the color of the line

        if contrast:
            # Convert the color to a perceived brightness value
            r, g, b = line.get_color()[:3]  # Extract RGB values
            brightness = (r * 299 + g * 587 + b * 114) / 1000  # Perceived brightness formula

            # Set text color based on brightness (black text for bright backgrounds, white text for dark backgrounds)
            text_color = 'white' if brightness < 0.5 else 'black'
        else:
            text_color = 'white'

        for it, (x_data, y_data) in enumerate(zip(line.get_xdata(), line.get_ydata())):
            if it % interval == 0:
                ax.text(
                    x_data, y_data, f'{y_data:.0f}', 
                    ha='center', va='bottom',
                    color=text_color,  # Set the chosen text color
                    bbox=dict(facecolor=line_color, edgecolor='none', alpha=0.8)  # Set background color matching the line color
                )


def plot_bar_labels(ax, contrast=False):
    if not isinstance(contrast, bool):
        raise TypeError(f"Expected 'contrast' to be of type 'bool', but got {type(contrast).__name__} instead.")
    
    for bar in ax.patches:
        height = bar.get_height()
        
        # Only add a label if the height is above a threshold (e.g., 0.1)
        if height > 0.01:  

            bar_color = bar.get_facecolor()  # Get the color of the bar
            
            if contrast:
                # Calculate perceived brightness of the bar color for contrast
                r, g, b = bar_color[:3] 
                brightness = (r * 299 + g * 587 + b * 114) / 1000  # Perceived brightness 
                text_color = 'white' if brightness < 0.5 else 'black'  # White text on dark bars, black text on bright bars
            else:
                text_color = 'white'

            # Label positioning
            height = bar.get_height()
            x_position = bar.get_x() + bar.get_width() / 2

            # Annotate the bar with the height value
            ax.text(
                x_position, height / 2, f'{height:.2f}',  # Set label at half the height of the bar
                ha='center', va='center', color=text_color,
                bbox=dict(facecolor=bar_color, edgecolor='none', alpha=0.8),  # Background color to match bar color
                fontsize=12) 
        



def create_time_xticks(ax,hour_interval=1, format='%m/%d %H:00', rotation=45):
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=hour_interval))
    ax.xaxis.set_major_formatter(mdates.DateFormatter(format))
    ax.tick_params(axis='x', rotation=rotation)