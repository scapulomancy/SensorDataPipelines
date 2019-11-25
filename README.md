# Data pre-processing for sensors (Spatial Data) 

This Module is a re-edition of Toulouse Heat Island. It is simpler and faster
thanks to the use of geopandas and shapely. It is designed to aggregate data around sensors. But it can
be useful to pre-process any kind of spatial data. Pre-processing are saved in pipelines, the advantage being
that pipelines can be replicated to pre-process several times one data-set changing one parameter or to
pre-process in the same way several data-sets.

So there are basically two useful parts in this project : 
  *  The implemented Pipeline structure : it is based on the pipe function of GeoDataFrames
  *  Functions to aggregate spatially (buffers) and temporally (time ranges) sensor data
  
## Typical workflow

Sensors locations are vector data, you need to be able to open this data in a GeoDataFrame.
The pipeline will contain operations on GeoDataFrames, we will define here an operation as :

`pipe_operation(GeoDataFrame, *args, **kwargs) -> GeoDataFrame`

Any operation could be used if it respects this convention. 


For example if you create a Pipeline `pipeline_UHI`, you can add 
an operation of aggregation of NDVI Raster Data with : 

```python
from src.data_preprocessing import *
pipeline_UHI = Pipeline('UHI_Analysis')
pipeline_UHI.add_operation('EVI Raster',
                           add_buffer_aggregated_data,
                           bands=[1],
                           buffer_size=0.005,
                           funcs=[np.mean,np.max,np.std],
                           file='data/EVI_Landsat8_29_07_2019.tif')
```

Then, just process a dataframe like this: 

``` python
gdf = geopandas.read_file('file.shp')
gdf_new = pipeline_UHI.process(gdf, out_file='pipeline',produce_file=True)
``` 

`out_file` is used to choose the name of the pipeline JSON description generated by process.
This JSON file can be used to initiate a Pipeline with the class method `Pipeline.from_json`. 
`produce_file` is a boolean to decide if you want the process to also generate a GeoJSON from the output
DataFrame. But a simpler way to do it and with more liberty is use to_file, that lets you choose
the driver (for example to produce Shapefiles or GPCK,...)

 ``` python
gdf_new.to_file('file_new.shp', driver='ESRI Shapefile')
``` 

## Aggregation Functions 

Here are some functions that could be useful in your pipelines. Note that with the Pipeline structure,
you can use any pandas or geopandas operations or any kind of operation you will define.

Data you can aggregate are : 
  * Rasters (example : aggregate the mean NDVI within buffers of sensors)
  * Vectors (example : aggregate the sum of buildings within buffers of sensors)
  * TimeSeries : A time series could be in the form of a Dataframe with timestamps, a directory of JSONs
  or a single JSON. It also required an index to be able to join the time series with sensors table 
  (example : aggregate the mean value of the records between 9:00 and 12:00 in November 2019 for each sensor)
  
### Spatial Operations included with the library

`add_buffer_aggregated_data`
```python
def add_buffer_aggregated_data(gdf, file, bands = None, columns = None, buffer_size=0.0025, funcs=(np.mean,np.max),out_file=None,out_driver='ESRI Shapefile'):
    '''
    Aggregate spatial data within buffers of geometries in a GeoDataFrame. This function can aggregate vector and raster data.
    For raster preferred format if GeoTif and for Vector, Shapefiles. But many other types are supported.
    To see what are these types, rasterio library is used for rasters and geopandas is used for vectors.
    Rasterio is based on gdal so possible types should be in https://gdal.org/drivers/raster/index.html.
    For Geopandas, GeoJson, Geopackage are also possible. The programm will read csv and xlsx but will be unable to get the spatial dimension of the data.
    Remark : You can always use the 'area' and 'bounds' columns in the case of vector data, it will be created for each geometry if it doesn't exist


    :param gdf: GeoDataFrames with sensors
    :type gdf: gpd.GeoDataFrame
    :param file: path to a file (raster or vector)
    :type file: str
    :param bands: should be used in the case of raster, choose the bands you want information about (bands nums are integers)
    :type bands: list
    :param columns: should be used in the case of vector, choose the columns you want information about (column names are strings)
    :type columns: list
    :param buffer_size: Size of the buffers in degrees, information will be aggregated in the buffered centroid of each geometries in gdf
    :type buffer_size: float
    :param funcs: a list of functions of aggregation (for example min, max), the scheme is f : iterable -> value (int,float,string,...)
    :type funcs: list
    :param out_file: path to out_file to write the result
    :type out_file: str
    :param out_driver: driver to use (default is a Shapefile), to change see shortnames of drivers in GDAL (https://gdal.org/drivers/vector/index.html)
    :type out_driver: str
    :return: gdf with additional columns corresponding to the aggregated statistics
    '''
    
    [...]
    
    return GeoDataFrame
``` 

`add_aggregated_data_neighbourhood`
```python
def add_aggregated_data_neighbourhood(gdf, file, columns = None, neighbourhood_size=1, funcs=(np.mean,np.max),out_file=None,out_driver='ESRI Shapefile'):
    '''
    Same as add_buffer_aggregated_data but the aggregation is not based on a buffer but on the closest polygon centroids. 
    Only works for vecto vectorial data.
    '''
    
    [...]
    
    return GeoDataFrame
``` 

### Time Series Aggregation

`add_aggregated_time_series`
```python
def add_aggregated_data_neighbourhood(gdf, file, columns = None, neighbourhood_size=1, funcs=(np.mean,np.max),out_file=None,out_driver='ESRI Shapefile'):
    def add_aggregated_time_series(gdf, data, column_time_stamps, columns, column_linked_sensors=None, sensor_index_list=None, date_range=None, hour_intervals=(('09:00:00','17:00:00'),('22:00:00','05:00:00')), aggregating_funcs=(np.mean,np.max), out_file=None, out_driver='ESRI Shapefile'):
    '''
    Aggregates sensor data time series of observations done by some sensors).
    There are options for time series filtering. Like selecting several hour intervals, selecting a date range.
    The inputs are a GeoDataFrame listing sensors (with at least an id for each sensor), a Directory with json containing the time series or a json file.
    To link the jsons and the GeoDataFrame index, you can choose between linking a features of the json with column_linked_sensors (best method) and indicating by yourself with a list
    what is the sensor index each json corresponds to in sensor_index_list (bad method).
    You have to indicate which column is going to be the time stamp of each measurement in the jsons.
    You also have to indicate which columns of the json are going to be aggregated with the argument column.

    :param gdf: GeoDataFrames with sensors
    :type gdf: gpd.GeoDataFrame
    :param data: path to a directory/file with jsons or dataframe
    :type data: str, pd.DataFrame
    :param column_time_stamps: Column in json with the timestamp (if nested json use a point to indicate the path in the json, for example : fields.measurement), to see
    what are the names of the columns, run the function json_normalize(file) of pandas on a json.
    :type column_time_stamps: str
    :param columns: Columns names to aggregate, same remark as column_time_stamps
    :type columns: list
    :param column_linked_sensors: if not None, name of the column of the json with the sensors ids (same remark as columns_time_stamps)
    :type column_linked_sensors: str
    :param sensor_index_list: Bad method, choose manually the index of each json.
    :type sensor_index_list: list
    :param date_range: Example : ('2019-07-08','2019-07-23'), if not None only data within this range will be considered
    :type date_range: tuple
    :param hour_intervals: Example : (('09:00:00','17:00:00'),('22:00:00','05:00:00')), goes through the aggregation for each time interval indicated
    :type hour_intervals: list
    :param aggregating_funcs: a list of functions of aggregation (for example min, max), the scheme is f : iterable -> value (int,float,string,...)
    :type aggregating_funcs: list
    :param out_file: path to out_file to write the result
    :type out_file: str
    :param out_driver: driver to use (default is a Shapefile), to change see shortnames of drivers in GDAL (https://gdal.org/drivers/vector/index.html)
    :type out_driver: str
    :return: gdf with additional columns corresponding to the aggregated statistics
    '''
    
    [...]
    
    return GeoDataFrame
``` 

### Interpolation

`inverse_distance_weighting`
```python
def inverse_distance_weighting(gdf, val_column, power, lon_step, lat_step, extent=None):
    '''
    Interpolation from points
    Returns points
    '''
    
    [...]
    
    return GeoDataFrame
``` 

`simple_kriging`
```python
def simple_kriging(gdf, val_column, lon_step, lat_step, extent=None, variogram_model='linear',verbose=False,):
    '''
    Interpolation from points
    Returns points
    '''
    
    [...]
    
    return GeoDataFrame
``` 

### Other functions

`drop_index_duplicates`
```python
def drop_index_duplicates(gdf):
    gdf['temp_col_index_temp'] = gdf.index
    gdf.drop_duplicates(subset='temp_col_index_temp',inplace=True)
    gdf.drop(columns=['temp_col_index_temp'])
    return gdf
``` 

### In Pandas

* `pd.DataFrame.dropna`
* `pd.DataFrame.set_index`
* ...

### Example of a custom function for to add a colored logend

 ```python
from src.data_preprocessing import *
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import ListedColormap, LinearSegmentedColormap
import ast

'''
Aggregation and coloring
'''


colors = ("green","yellow","orange","red") #You can aslo use rgb [(0, 1, 0), (1, 1, 0), (1, 0.6, 0),(1,0,0)]

def get_cm(name,c,n):
    return LinearSegmentedColormap.from_list(name, c, N=n)
def color_map_entry(hex_color, value):
    return """              <ColorMapEntry color="{}" quantity="{}" />\n""".format(hex_color,value)

def get_styling_geoserver(hex_colors, values):
    template = open('template_style.txt', "r")
    style = open("style.txt", "w")
    first = True
    for line in template:
        style.write(line)
        if '<ColorMap>' in line and first:
            for hex_color, value in zip(hex_colors, values):
                style.write(color_map_entry(hex_color,value))
            first = False

def get_styling_cavi(hex_colors, values):
    d = {str(v) : c for v,c in zip(values, hex_colors)}
    return d

def to_hex(rgb_colors):
    return [matplotlib.colors.to_hex(c) for c in rgb_colors]

def get_intervals(df,n,column,colors=colors):
    '''
    Will write the geoserver styling and print the cavi style and return a gdf with an interval column
    '''
    intervals = pd.cut(df[column],n).sort_values().unique().to_list()
    intervals_index = pd.IntervalIndex(intervals)
    intervals_list = intervals_index.to_series().astype(str).apply(lambda r: r.replace('(', '[')).tolist()
    values = [i.left for i in intervals]
    df['intervals_'+column] = pd.cut(df[column], intervals_index).astype(str).apply(lambda r: r.replace('(', '['))
    colormap = get_cm("colormap",colors,n)
    colors = [matplotlib.colors.to_hex(colormap(i/n)) for i in range(n)]
    print(get_styling_cavi(colors, intervals_list))
    get_styling_geoserver(colors, values)
    return df



gdf = gpd.read_file('manhattan_buildings_full_crop.shp')

pipeline_colors = Pipeline('Colors')


pipeline_colors.add_operation('PM Raster',
                           add_buffer_aggregated_data,bands=[1],
                           buffer_size=50,
                           funcs=[np.mean],
                           file='PM_0_extended_reclassified.tif')
pipeline_colors.add_operation('Cleaning Empty Rows',
                           pd.DataFrame.dropna,
                           subset=['mean_PM_0_extended_reclassified_band1'])
pipeline_colors.add_operation('Create Legend',
                              get_intervals,
                              n=20,
                              column='mean_PM_0_extended_reclassified_band1')


gdf_new : gpd.GeoDataFrame = pipeline_colors.process(gdf)
gdf_new.to_file('manhattan_buildings_full_PM.shp')
``` 










