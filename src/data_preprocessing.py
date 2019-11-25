import geopandas as gpd
import rasterio
from rasterio import crs
import fiona
import os
import pykrige as pk
import json
import numpy as np
import itertools
import pandas as pd
from pandas.io.json import json_normalize
from rasterio import mask
import numpy.ma as ma
import warnings
# To run correctly GDAL, you have to indicate the Library\share\gdal directory
# os.environ['GDAL_DATA'] = r"path"

"""
************************Pipelines Class***********************
"""

class Pipeline():
    '''
    This class allows to create a pipeline of operations on GeoDataFrames and execute them to replicate
    a data pre-processing step easily.

    See the pp_pipeline.py in examples

    '''
    def __init__(self,name,valid=False,pipe=None):
        self.name = name
        if pipe is None:
            self._pipe = {'pipe': name, 'operations':[]}
        else:
            self._pipe=pipe
        self._valid = valid

    @classmethod
    def from_json(cls,filename):
        '''
        Classmethod constructor from a json description of the pipeline

        :param filename: json
        :return: Pipeline
        '''
        with open(filename) as json_file:
            data = json.load(json_file)
        return cls(filename,valid=False,pipe=data)

    def to_json(self,name=None):
        if name is None:
            name = self.name
        with open('{}.json'.format(name), 'w') as out:
            json.dump(self._pipe, out,cls=CustomEncoder, indent=4)
            print('_pipe saved in {}.json'.format(name))

    def get_pipe(self):
        return self._pipe

    def set_pipe(self,pipe):
        self._valid = False
        self._pipe = pipe

    def add_operation(self,name,function,*args,**kwargs):
        self._pipe['operations'].append({'name':name,'function': function,'args': args, 'kwargs': kwargs })
        self._valid = False

    def pop_operation(self,name):
        for i in range(len(self._pipe)):
            if self._pipe['operations'][i]['name'] == name:
                self._valid = False
                return self._pipe.pop(i)
        return None

    def move_operation(self, name, position):
        for i,el in enumerate(self._pipe['operations']):
            if el['name'] == name:
                self._valid = False
                self._pipe['operations'].insert(position,self._pipe['operations'].pop(i))
                break
        else:
            raise Exception('Value {} not found in pipe'.format(name))

    def set_kwarg(self, op_name, kwarg_name, new_value):
        for op in self._pipe['operations']:
            if op['name'] == op_name:
                for kwarg in op['kwargs']:
                    if kwarg == kwarg_name:
                        op['kwargs'][kwarg_name]=new_value
        return None

    def process(self, gdf, produce_file = False, out_file = None):
        '''
        Runs the pipeline on a gdf.
        Also creates a json description of the pipeline.

        :param gdf: GeoDataFrames
        :param produce_file: if True, the process will generate an output GeoJson
        :param out_file: Name of the output GeoJson
        :return: a GeoDataFrame
        '''
        print('{} in process ...'.format(self.name))
        for op in self._pipe['operations']:
            print('Operation : {}'.format(op['name']))
            print('GeoDataFrame initial size : {}'.format(str(gdf.shape)))
            print('GeoDataFrame Head :')
            print(gdf.head())
            function,args, kwargs = self.get_args(op)
            gdf = gdf.pipe(function,*args,**kwargs)
            if type(gdf) != gpd.GeoDataFrame:
                raise TypeError('Pipe operation must return a GeoDataFrame not {} !'.format(type(gdf)))
        self._valid = True
        print('End of Pipe')
        print('GeoDataFrame initial size : {}'.format(str(gdf.shape)))
        print('GeoDataFrame Head :')
        print(gdf.head())
        if out_file is None:
            pipe_out_name = '{}_pipe.json'.format(self.name)
        else:
            base = os.path.basename(out_file)
            out_name=os.path.splitext(base)[0]
            pipe_out_name = '{}_pipe.json'.format(out_name)
        with open(pipe_out_name, 'w') as out:
            json.dump(self._pipe, out,cls=CustomEncoder, indent=4)
            print(pipe_out_name)
        if produce_file:
            gdf.to_file(out_file, driver='GeoJSON')
            print(out_file)
        return gdf

    def process_generator(self, gdf, op_name, modified_kwarg, list_of_values):
        '''
        Calls several times the process of the pipeline on a gdf, one operation can be modified at each step.
        Creates geojson files at each iteration.

        :param gdf: GeoDataFrames
        :param op_name: Name of the operation that will be modified at each operation
        :param modified_kwarg: name of the kwarg to modify
        :param list_of_values: each value of this list corresponds to the value of modified_kwarg during an iteration
        :return:
        '''
        for value in list_of_values:
            gdf_bis = gdf.copy()
            self.set_kwarg(op_name, modified_kwarg,value)
            out_name = 'pipe_result_{0}_{1}.geojson'.format(modified_kwarg,value)
            self.process(gdf_bis,produce_file=True,out_file=out_name)

    def get_args(self, pipe_element):
        res = (pipe_element['function'],pipe_element['args'],pipe_element['kwargs'])
        return res

'''
*************************Spatial and Temporal Aggregation of Sensor Data for pipeline***************************
'''

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
    if not gdf.crs:
        raise MissingCRS('No CRS found ! Set SensorsDataFrame.crs with set_crs(epsg=epsg).')
    dataset, data_type = open_spatial_data(file)
    base = os.path.basename(file)
    name = os.path.splitext(base)[0]
    if data_type == 'raster':
        if bands is None:
            bands = [i for i in range(dataset.count)]
        epsg = dataset.crs.to_epsg()    # Get EPSG of the raster
        df = gdf.to_crs(epsg=epsg)     # New reprojected df
        df['buffers'] = df.centroid.buffer(buffer_size, resolution=16)  # Adding buffers
        new_stats_index = []
        for func, band in list(itertools.product(funcs, bands)):
            new_stats_index += ['_'.join([func.__name__,name,'band'+str(band)])]
        print('Aggregating ...')
        new_stats = df[['buffers']].apply(lambda r: get_stats_raster(dataset, r['buffers'], bands, funcs, new_stats_index), axis=1)
        res = df.join(new_stats).drop(columns=['buffers'])
        if out_file is not None:
            try:
                res.to_file(out_file, driver=out_driver)
            except Exception as e:
                print(e)
        return res
    elif data_type == 'vector':
        if columns is None:
            raise ValueError('Missing argument columns for buffering raster_data : list of columns in '+str(dataset.columns.to_list()))
        for col in columns:
            if col not in dataset.columns:
                if col == 'area':
                    dataset['area']=dataset.area
                elif col == ['bounds']:
                    dataset['bounds']=dataset.bounds
                else:
                    raise ValueError('Columns {} does not exist in {}, existing columns : {} and generated columns {}'.format(col, file,str(dataset.columns),str(['area','bounds'])))
        epsg = get_crs_gdf(dataset)     # Get EPSG of new DataFrame
        espg_main = get_crs_gdf(gdf)
        df = gdf.to_crs(epsg=epsg)     # New reprojected df
        df['buffers'] = df.centroid.buffer(buffer_size, resolution=16)  # Adding buffers
        xmin, ymin, xmax, ymax = gdf.total_bounds
        dataset = dataset.cx[xmin:xmax, ymin:ymax]
        new_stats_index = []
        for func, col in list(itertools.product(funcs, columns)):
            new_stats_index += ['_'.join([func.__name__,name, col])]
        print('Aggregating ...')
        new_stats = df[['buffers']].apply(lambda r: get_stats_vector(dataset,r['buffers'],columns,funcs,new_stats_index),axis=1)
        res = df.join(new_stats).drop(columns=['buffers'])
        if out_file is not None:
            try:
                res.to_file(out_file,driver=out_driver)
            except Exception as e:
                print(e)
        return res


def add_aggregated_data_neighbourhood(gdf, file, columns = None, neighbourhood_size=1, funcs=(np.mean,np.max),out_file=None,out_driver='ESRI Shapefile'):
    if not gdf.crs:
        raise MissingCRS('No CRS found ! Set SensorsDataFrame.crs with set_crs(epsg=epsg).')
    dataset, data_type = open_spatial_data(file)
    base = os.path.basename(file)
    name = os.path.splitext(base)[0]
    if data_type == 'raster':
        raise Exception('This function is not avalaible for rasters but only for vectors')
    elif data_type == 'vector':
        if columns is None:
            raise ValueError('Missing argument columns for buffering raster_data : list of columns in '+str(dataset.columns.to_list()))
        for col in columns:
            if col not in dataset.columns:
                if col == 'area':
                    dataset['area']=dataset.area
                elif col == ['bounds']:
                    dataset['bounds']=dataset.bounds
                else:
                    raise ValueError('Columns {} does not exist in {}, existing columns : {} and generated columns {}'.format(col, file,str(dataset.columns),str(['area','bounds'])))
        epsg = get_crs_gdf(dataset)     # Get EPSG of new DataFrame
        espg_main = get_crs_gdf(gdf)
        df = gdf.to_crs(epsg=epsg)     # New reprojected df
        df['centroids']=df.centroid  # Adding buffers
        xmin, ymin, xmax, ymax = gdf.total_bounds
        dataset = dataset.cx[xmin:xmax, ymin:ymax]
        new_stats_index = []
        for func, col in list(itertools.product(funcs, columns)):
            new_stats_index += ['_'.join([func.__name__,name, col])]
        print('Aggregating ...')
        new_stats = df[['centroids']].apply(lambda r: get_stats_vector_neighbourhood(dataset,r['centroids'],columns,funcs,new_stats_index,neighbourhood_size),axis=1)
        res = df.join(new_stats).drop(columns=['centroids'])
        if out_file is not None:
            try:
                res.to_file(out_file,driver=out_driver)
            except Exception as e:
                print(e)
        return res

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
    if bool(sensor_index_list) == bool(column_linked_sensors):
        raise Exception('Must specify column_linked_sensors or sensor_index_list but not both !')
    df = pd.DataFrame()
    if isinstance(data, str):
        if os.path.isdir(data):
            df = open_dir_of_data(data, sensor_index_list=sensor_index_list)
        elif os.path.isfile(data):
            df = open_data(data)
    elif isinstance(data, pd.DataFrame):
        df = data
    else:
        raise Exception('Type of data must be : path to a file or pd.DataFrame !')
    df[column_time_stamps] = pd.to_datetime(df[column_time_stamps])
    if column_linked_sensors:
        if column_linked_sensors not in df.columns:
            raise Exception('{} not in columns : {}'.format(column_linked_sensors,str(df.columns.to_list())))
        df.rename(columns={column_linked_sensors:'id_sensor'},inplace=True)
    df.set_index([column_time_stamps,'id_sensor'],inplace=True)
    if date_range:
        beg, end = date_range
        df = df.xs(slice(beg,end), level=0, drop_level=False)
    if df.empty:
        raise Exception('Empty DataFrame for slice of dates : {}'.format(date_range))
    gdf['id_sensor']=gdf.index
    print('Indices in imported data :')
    print(df.index.get_level_values('id_sensor').unique())
    print('Indices of sensors :')
    print(np.sort(gdf['id_sensor'].unique()))
    print('Aggregating ...')
    new_stats = gdf['id_sensor'].apply(lambda r: get_stats_temporal(df.iloc[df.index.get_level_values('id_sensor') == r], hour_intervals,columns, aggregating_funcs,r))
    new_columns = pd.Index(['_'.join(col) for col in new_stats.columns])
    print(new_columns)
    new_stats.columns = new_columns
    res = gdf.join(new_stats)
    if out_file is not None:
        try:
            res.to_file(out_file, driver=out_driver)
        except Exception as e:
            print(e)
    print(res.columns.to_list())
    return res


'''
Regression
'''



'''
Interpolation
'''
def euclidian_dist(p1,p2):
    x1,y1 = p1
    x2,y2 = p2
    res = np.sqrt((x1-x2)**2+(y1-y2)**2)
    return res

def inverse_distance_weighting(gdf, val_column, power, lon_step, lat_step, extent=None):
    if extent is None:
        extent = gdf.total_bounds
    lon = gdf.geometry.x.values
    lat = gdf.geometry.y.values
    values = gdf[val_column].fillna(0).values
    lon_min, lat_min, lon_max, lat_max = extent
    grid_lon = np.arange(lon_min, lon_max, lon_step)
    grid_lat = np.arange(lat_min, lat_max, lat_step)
    coordinates = list(zip(*itertools.product(grid_lat, grid_lon)))
    lats, lons = list(coordinates[0]), list(coordinates[1])
    new_values = np.zeros(len(lats))
    i = 0
    for y,x in zip(lats,lons):
        p1 = (x,y)
        distances = [euclidian_dist(p1,p2) for p2 in zip(lon,lat)]
        new_values[i] = sum([v/(d**power) for d,v in zip(distances,values)])/sum([1/(d**power) for d in distances])
    new_gdf = gpd.GeoDataFrame(geometry=gpd.points_from_xy(lons, lats))
    new_gdf[val_column + '_idw'] = new_values
    new_gdf.crs = gdf.crs
    return new_gdf


def simple_kriging(gdf, val_column, lon_step, lat_step, extent=None, variogram_model='linear',verbose=False,):
    '''
    Implements Ordinary Kriging and return the resulting grid in a new GeoDataFrame

    :param gdf:
    :param val_column:
    :param lon_step:
    :param lat_step:
    :param extent:
    :param variogram_model:
    :param verbose:
    :return:
    '''
    if extent is None:
        extent = gdf.total_bounds
    lon = gdf.geometry.x.values
    lat = gdf.geometry.y.values
    values = gdf[val_column].fillna(0).values
    lon_min, lat_min, lon_max, lat_max = extent
    grid_lon = np.arange(lon_min, lon_max, lon_step)
    grid_lat = np.arange(lat_min, lat_max, lat_step)
    OK = pk.OrdinaryKriging(lon, lat, values, variogram_model=variogram_model,verbose=verbose, enable_plotting=False)
    z, ss = OK.execute('grid', grid_lon, grid_lat)
    coordinates = list(zip(*itertools.product(grid_lat,grid_lon)))
    lats, lons = list(coordinates[0]), list(coordinates[1])
    values = np.ndarray.flatten(np.array(z))
    new_gdf = gpd.GeoDataFrame(geometry=gpd.points_from_xy(lons,lats))
    new_gdf[val_column+'_krig']=values
    new_gdf.crs = gdf.crs
    return new_gdf




'''
************************Utilities**************************
'''
def open_dir_of_data(directory, sensor_index_list=None):
    df = pd.DataFrame()
    for i, file in enumerate(os.listdir(directory)):
        if file.endswith(".json"):
            if i == 0:
                df = open_sensor_records(os.path.join(directory, file))
                if sensor_index_list:
                    df['id_sensor'] = [sensor_index_list[0]] * len(df)
            else:
                df2 = open_sensor_records(os.path.join(directory, file))
                if sensor_index_list:
                    df2['id_sensor'] = [sensor_index_list[i]] * len(df)
                df = df.append(df2,sort=True)
    return df

def open_data(file):
    try:
        df = pd.read_csv(file)
    except Exception as e1:
        print('Not a csv : {}'.format(e1))
        try:
            df = pd.read_json(file)
        except Exception as e2:
            print('Not a json : {}'.format(e2))
            try:
                df = pd.read_excel(file)
            except Exception as e3:
                print('Not an excel : {}'.format(e3))
                raise Exception('Cannot open file {} !'.format(file))
    return df



def open_spatial_data(file):
    '''
    Open any kind of spatial data possible with rasterio for rasters and GeoPandas for vectors.

    :param file: path of the file to open
    :return: an object of type raster for rasterio and of type GeoDataFrame for GeoPandas
    '''
    print('Opening {} ...'.format(file))
    try:
        dataset = rasterio.open(file)
        data_type = 'raster'
    except rasterio.errors.RasterioIOError as e1:
        try:
            dataset = gpd.read_file(file)
            original_size = dataset.size
            print('Dropping empty and non valid geometries !')
            dataset = dataset[dataset.geometry.is_empty == False]
            new_size = dataset.size
            if new_size != original_size:
                warnings.warn('Some empty geometries have been ignored ! (use is_empty to see these geometries)')
            dataset = dataset[dataset.geometry.is_valid]
            new_new_size = dataset.size
            if new_new_size != new_size:
                warnings.warn('Some invalid geometries have been ignored ! (use is_valid to see these geometries)')
            data_type = 'vector'
        except fiona.errors.DriverError as e2:
            raise DataIOError([
                e1,
                e2
            ])
    return dataset, data_type

def get_stats_vector(dataset,buffer,columns,funcs,stats_name):
    '''
    Return GeoSeries of stats about data within a buffer. If the buffer is empty, gives np.nan.

    :param dataset: GeoDataFrame containing spatial data
    :param buffer: shapely geometry, the function will only take in account spatial data intersecting this buffer
    :param columns: columns of dataset that will be mapped
    :param funcs: mapping functions list, for example ['min'] will give the min of each columns for geometries within the buffer
    :param stats_name : list of the names of stats, keep in mind that it has to be the product of func names and column names
    :return: a GeoSeries with every stat
    '''
    print('Get stats vector ...')
    df = geometries_interesect(dataset,buffer)[columns]
    if df.empty:
        return gpd.GeoSeries()
    else:
        res = df.agg(funcs).stack()
        res.reset_index(drop=True)
        res.index = stats_name
        return res

def get_stats_raster(dataset,buffer,bands,funcs,stats_name):
    '''
    See get_stats_vector
    '''
    try:
        masked, tranform = rasterio.mask.mask(dataset,[buffer],crop=True,nodata=0)
    except ValueError as e:
        print('Out of Raster Geometry ignored !')
        res = pd.Series([np.nan]*len(funcs))
        res.reset_index(drop=True)
        res.index = stats_name
        return res
    if len(bands) == 1:
        m = (ma.masked_equal(masked, 0)).compressed()
    else:
        m = [(ma.masked_equal(masked[i], 0)).compressed().tolist() for i in range(len(masked)) if i in bands]
    df = pd.DataFrame(m)
    if df.empty:
        return gpd.GeoSeries()
    else:
        res = df.agg(funcs).stack()
        res.reset_index(drop=True)
        res.index = stats_name
        return res

def get_stats_vector_neighbourhood(dataset,centroid,columns,funcs,stats_name,n):
    '''
    Return GeoSeries of stats about data within a buffer. If the buffer is empty, gives np.nan.

    :param dataset: GeoDataFrame containing spatial data
    :param buffer: shapely geometry, the function will only take in account spatial data intersecting this buffer
    :param columns: columns of dataset that will be mapped
    :param funcs: mapping functions list, for example ['min'] will give the min of each columns for geometries within the buffer
    :param stats_name : list of the names of stats, keep in mind that it has to be the product of func names and column names
    :return: a GeoSeries with every stat
    '''
    print('Get stats vector ...')
    dataset['distance_to_centroid'] = dataset.geometry.apply(lambda r: r.distance(centroid))
    df = dataset.nsmallest(n,'distance_to_centroid')[columns]
    if df.empty:
        return gpd.GeoSeries()
    else:
        res = df.agg(funcs).stack()
        res.reset_index(drop=True)
        res.index = stats_name
        return res


def get_stats_temporal(temporal_data, hour_intervals, columns, funcs,index):
    '''
    See get_stats_vector
    '''
    print('Sensor Id {0} processing ({1} entries)...'.format(index,temporal_data.shape))
    df = temporal_data.droplevel('id_sensor')
    first = True
    stats = gpd.GeoSeries()
    for interval in hour_intervals:
        if first:
            beg,end = interval
            res = df.between_time(beg,end)
            new_columns = [col + str(interval) for col in columns]
            res.rename(columns={col: new_col for col, new_col in zip(columns, new_columns)}, inplace=True)
            stats = res[new_columns].agg(funcs).stack()
            first = False
        else:
            beg, end = interval
            res = df.between_time(beg, end)
            new_columns = [col+str(interval) for col in columns]
            res.rename(columns={col:new_col for col,new_col in zip(columns, new_columns)}, inplace = True)
            stats = res[new_columns].agg(funcs).stack()
    return stats


def get_crs_gdf(gdf):
    return crs.CRS.from_dict(gdf.crs).to_epsg()

def geometries_interesect(dataset, clip_geom):
    '''
    Gather geometries of a vector layer (based on intersection with clip_geom)

    :param dataset:
    :param clip_geom:
    :return:
    '''
    mask = dataset.geometry.intersects(clip_geom)
    return dataset.loc[mask].fillna(0)

def open_sensor_records(file):
    filename, file_extension = os.path.splitext(file)
    if file_extension == '.csv':
        df = pd.read_csv(file)
        df.drop(df.columns[df.columns.str.contains('unnamed', case=False)], axis=1, inplace=True)
    elif file_extension == '.json':
        print('json file !')
        print(file)
        data_set_json = open(file, 'r')
        data_set_sensors = json.load(data_set_json)
        df = json_normalize(data_set_sensors)
    return df

'''
*******************************Explore Temporal Data***************************
'''

def do_time_analysis_sensors(df, time_column, sensor_index_column):
    indices = df[sensor_index_column].unique()
    stats = pd.DataFrame()
    mins=[]
    maxs=[]
    ids =[]
    for i in indices:
        subset = df[df[sensor_index_column] == i]
        mins += [subset[time_column].min()]
        maxs += [subset[time_column].max()]
        ids += [i]
    stats['maxs'] = maxs
    stats['mins'] = mins
    stats['id_sensors']=ids
    print(stats)
    print('Maxs of stats')
    print(stats.max())
    print('Mins of stats')
    print(stats.min())

'''
***************************Other Operations for pipelines********************
'''

def drop_index_duplicates(gdf):
    gdf['temp_col_index_temp'] = gdf.index
    gdf.drop_duplicates(subset='temp_col_index_temp',inplace=True)
    gdf.drop(columns=['temp_col_index_temp'])
    return gdf

'''
*************************Custom Exceptions*************************
'''



class DataIOError(Exception):
    """
    Raised if data is not spatial data (either vector or raster) that can be opened with rasterio or fiona
    """
    pass

class MissingCRS(Exception):
    """
    Raise when info about CRS lacks (in the case of operations on spatial data like aggregation within buffers)
    """
    pass

class UnvalidatedPipeline(Exception):
    '''
    Raise when trying to run unvalidated pipeline
    '''
    pass

class ColumnException(Exception):
    '''
    When a specified column does not exist in a DataFrame
    '''
    pass

'''
****************************Custom Encoder***********************
'''

class CustomEncoder(json.JSONEncoder):

    def default(self, object):
        if callable(object):
            return object.__name__
        else:
            return json.JSONEncoder.default(self, object)
