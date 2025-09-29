"""Utility functions around IO operations on filesystems."""
import base64
import io
import json
import os
import pickle as pkl
import platform
import shutil
from glob import glob
import joblib

import pandas as pd
import yaml

def local_folder_s3():
    local_folder_path = "tmp/"
    return local_folder_path

def is_colab():
    try:
        import google.colab  # noqa

        return True
    except ModuleNotFoundError:
        return False


def is_win(path):
    if (path[1] == ":") and (path[0].isupper()):
        return True
    else:
        return False


def is_linux(path):
    if path.startswith("/"):
        return True
    else:
        return False


def win_to_linux(path):
    if is_linux(path):
        return path
    path = path.split(":")
    path[0] = "/mnt/" + path[0].lower()
    path = "".join(path)
    path = path.replace("\\", "/")
    return path


def linux_to_win(path):
    if is_win(path):
        return path
    path = path.replace("/mnt/", "")
    path = path.split("/")
    path[0] = path[0].upper() + ":"
    path = "\\".join(path)
    return path

def adj_path(path):
    path_ = path
    try:
        if is_win(path):
            if platform.system().lower() == "linux":
                path = win_to_linux(path)
        elif is_linux(path):
            if platform.system().lower() == "windows":
                path = linux_to_win(path)
        return path
    except IndexError as e:
        return path_

def read_preprocess(df):
    df = df.drop(columns=["Unnamed: 0"], errors="ignore")
    if "date" in df.columns:
        df = df.pd_cols_to_datetime()
    return df

def read(filepath, concat=None, **kwargs):
    """
    Reads data from various file types and sources.

    Supported file extensions and their corresponding data types:
    - .xlsx, .xls: A dictionary of pandas DataFrames.
    - .csv: A single pandas DataFrame.
    - .parquet: A single pandas DataFrame.
    - .feather: A single pandas DataFrame.
    - .sas7bdat: A single pandas DataFrame.
    - .pkl: Any Python object that can be pickled.
    - .ubj: Any Python object that can be converted to UBJSON.
    - .yml|.yaml: Any Python object that can be converted to YAML.

    Parameters
    ----------
    filepath : str
        The path or URL to the file to be read.
    concat : str or list of str, default None
        If the filepath points to a directory, specifies the file extension(s)
        to use for concatenating multiple files. If None and the filepath points
        to a directory, defaults to 'parquet'.
    **kwargs : dict
        Additional keyword arguments to be passed to the appropriate
        file-reading function.

        If `filepath` points to a SQL database and `conn` is provided as a keyword
        argument, `pd.read_sql` will be used to read the data from the database.

        If `filepath` points to a pickled model file and `model` is provided as a
        keyword argument, the model will be loaded using the provided class or function.
        If `assign_model` is True, the loaded model will be returned directly. Otherwise,
        the `model` object will be modified in-place and returned.

    Returns
    -------
    pd.DataFrame or object
        The data read from the file or source.

    Raises
    ------
    AssertionError
        If `filepath` is not a string.

    Examples
    --------
    >>> df = read(filepath='data.csv')
    >>> df = read(filepath='data.parquet')
    >>> df = read(filepath='data.pkl', model=MyModel, assign_model=True)
    >>> df = read(filepath='data.xlsx', sheet_name='Sheet1')
    >>> df = read(filepath='data_folder', concat='csv')

    """
    assert isinstance(filepath, str)
    global storage_options, root
    filepath = adj_path(filepath)

    ext = os.path.splitext(filepath)[1][1:]

    if ext == "" and concat is None:
        concat = "parquet"
    if (concat is not None) and (concat != "parquet"):
        if isinstance(concat, str):
            concat = [concat]
        files = []
        folder_path = f"{filepath}/"
        for file in concat:
            files.extend(glob(f"{folder_path}/*{file}"))
        if files:
            df = pd.concat([read(filepath=f, **kwargs) for f in files])
            return df
        else:
            return pd.DataFrame()
    elif concat == "parquet":
        return read_preprocess(
            pd.read_parquet(filepath, storage_options=storage_options, **kwargs)
        )

    assert isinstance(filepath, str)

    if "conn" in kwargs.keys():
        # read(filepath=<query_str>, conn=con_obj)
        return pd.read_sql(sql=filepath, con=kwargs["conn"], **kwargs)

    elif "model" in kwargs.keys():
        model = kwargs["model"]
        if ext in ["pkl", "pickle", "joblib"]:
            filepath = open(filepath, "rb")
        try:
            if "assign_model" in kwargs.keys() and kwargs["assign_model"]:
                model = model.load_model(filepath)
            else:
                model.load_model(filepath)
        except AttributeError:
            if "assign_model" in kwargs.keys() and kwargs["assign_model"]:
                model = model.load(filepath)
            else:
                model.load(filepath)
        shutil.rmtree(local_folder_s3(), ignore_errors=True)
        # filepath.close()
        return model

    elif ext in ["xlsx", "xls"]:
        df = pd.read_excel(filepath, storage_options=storage_options, **kwargs)
        return read_preprocess(df)
    elif ext == "csv":
        df = pd.read_csv(filepath, storage_options=storage_options, **kwargs)
        return read_preprocess(df)
    elif ext == "parquet":
        df = pd.read_parquet(filepath, storage_options=storage_options, **kwargs)
        return read_preprocess(df)
    elif ext == "sas7bdat":
        df = pd.read_sas(filepath, storage_options=storage_options, **kwargs)
        df[df.select_dtypes("O").columns] = df.select_dtypes("O").apply(
            lambda x: x.str.decode("utf-8") if isinstance(x[0], bytes) else x
        )
        return read_preprocess(df)
    elif ext == "feather":
        df = pd.read_feather(filepath, storage_options=storage_options, **kwargs)
        return df

    elif ext in ["pkl", "yml", "yaml", "pickle", "joblib"]:
        f = open(filepath, "rb")
        if ext in ["pkl", "pickle"]:
            df = pkl.load(f)
        elif ext == "joblib":
            model = joblib.load(filepath)
            return model
        else:
            df = yaml.safe_load(f)
        return df
    elif ext in ["txt"]:
        return open(filepath, "r").read()


def write(data, filepath, **kwargs):
    """
    Write data to a file specified by `filepath`. The type of file is inferred from the file extension.

    Supported file extensions and their corresponding data types:
    - .xlsx, .xls: A dictionary of pandas DataFrames or an pandas DataFrame.
    - .csv: A single pandas DataFrame.
    - .parquet: A single pandas DataFrame.
    - .feather: A single pandas DataFrame.
    - .pkl: Any picklable object.
    - .yml|.yaml: Any object that can be converted to YAML.
    - .ubj: Any model object.


    Parameters:
    -----------
    data : pandas DataFrame, dictionary, or Python object
        The data to be written to the file. The type of data depends on the file extension.
    filepath : str
        The path to the file.
    **kwargs : keyword arguments
        Additional keyword arguments to be passed to the file-writing functions. These arguments
        depend on the file extension. For example, when writing to a CSV file, you can pass `sep=', '`
        to specify the delimiter. If writing to a file with extension ".ubj", the `model` parameter can be
        passed as a keyword argument to specify the data model to be used for writing.

    Raises:
    -------
    NotImplementedError
        If the file extension is not one of the supported types.

    Returns:
    --------
    None
        This function only writes data to a file, it doesn't return anything.

    Examples:
    ---------
    >>> write(data=data, filepath='data.csv', index=False) # writes dataframe to a csv file
    >>> write(data=data, filepath='data.parquet') # writes dataframe to a parquet file
    >>> write(data=my_model, filepath='data.pkl', model=my_model) # writes the model object to a pickle file
    >>> write(data=data, filepath='data.xlsx') # writes dataframe to a sheet in an xlsx file
    >>> write(data=dict('sheet1':data1,'sheet2':data2),filepath='data.xlsx') # writes multiple sheets into an excel file.

    """
    assert isinstance(filepath, str)
    filepath = adj_path(filepath)

    os.makedirs(os.path.abspath(os.path.join(filepath, os.pardir)), exist_ok=True)

    ext = filepath.split(".")[-1]
    if ext not in [
        "xlsx",
        "xls",
        "csv",
        "parquet",
        "yml",
        "yaml",
        "pkl",
        "pickle",
        "ubj",
        "joblib",
    ]:
        raise NotImplementedError
    if ext in ["xlsx", "xls"]:
        if isinstance(data, dict):
            with pd.ExcelWriter(filepath, storage_options=storage_options) as wr:
                for sheet in data.keys():
                    if isinstance(data[sheet], pd.DataFrame):
                        try:
                            data[sheet].to_excel(
                                wr, sheet, storage_options=storage_options, **kwargs
                            )
                        except ValueError as e:
                            temp_path = (
                                os.path.join(
                                    filepath.replace(os.path.basename(filepath), ""),
                                    sheet,
                                )
                                + ".parquet"
                            )
                            write(data=data[sheet], filepath=temp_path)
                            print(
                                os.path.basename(filepath),
                                sheet,
                                e,
                                ", saved sheet in parquet",
                            )
                            temp_path = (
                                os.path.join(
                                    filepath.replace(os.path.basename(filepath), ""),
                                    sheet,
                                )
                                + ".parquet"
                            )
                            write(data=data[sheet], filepath=temp_path)
                            print(
                                os.path.basename(filepath),
                                sheet,
                                e,
                                ", saved sheet in parquet",
                            )
                            pass
                    elif isinstance(data[sheet], dict):
                        col = 0
                        row = 0
                        axis = data[sheet]["axis"]
                        assert isinstance(data[sheet]["dfs"], list)
                        if axis == 1 or axis == "row":
                            for df_ in data[sheet]["dfs"]:
                                df_.to_excel(
                                    wr,
                                    sheet,
                                    startrow=row,
                                    storage_options=storage_options,
                                    **kwargs,
                                )
                                row += len(df_) + 1 + df_.columns.nlevels
                        elif axis == 0 or axis == "column":
                            for df_ in data[sheet]["dfs"]:
                                df_.to_excel(
                                    wr,
                                    sheet,
                                    startcol=col,
                                    storage_options=storage_options,
                                    **kwargs,
                                )
                                col += len(df_.columns) + 1 + df_.index.nlevels
        else:
            assert isinstance(data, pd.DataFrame)
            data.to_excel(filepath, storage_options=storage_options, **kwargs)
    elif ext == "csv":
        assert isinstance(data, pd.DataFrame)
        data.to_csv(filepath, storage_options=storage_options, **kwargs)
    elif ext == "parquet":
        assert isinstance(data, pd.DataFrame)
        try:
            data.to_parquet(filepath, storage_options=storage_options, **kwargs)
        except Exception as e:
            object_columns = str(e).split("column ")[1].split(" ")[0]
            data[object_columns] = data[object_columns].astype(str)
            write(data=data, filepath=filepath, **kwargs)

    elif ext == "feather":
        assert isinstance(data, pd.DataFrame)
        data.to_feather(filepath, storage_options=storage_options, **kwargs)
    elif ext in ["pkl", "yml", "yaml", "joblib"]:
        f = (
                open(filepath, "wb")
                if ext in ["pkl", "joblib"]
                else open(filepath, "w")
            )
        f = (
            open(filepath, "wb")
            if ext in ["pkl", "joblib"]
            else open(filepath, "w")
        )
        if ext == "pkl":
            pkl.dump(data, f, **kwargs)
        elif ext in ["yml", "yaml"]:
            yaml.dump(dict(data), f)
        elif ext == "joblib":
            joblib.dump(data, f, **kwargs)
        f.close()
    elif ext == "ubj":
        data.save_model(filepath)


def listdir(path):
    """
    Return a list of files in a directory specified by `path`.

    Parameters:
    -----------
    path : str
        The path to the directory.

    Returns:
    --------
    A list of strings
        The names of the files in the directory.

    Raises:
    -------
    None

    Notes:
    ------
    This function uses the `os` and `S3FileSystem` modules to list the files in a directory.
    If the directory is on S3, it uses the `S3FileSystem` module, otherwise it uses the `os` module.
    """
    assert isinstance(path, str)
    path = adj_path(path)
    return os.listdir(path)


def isdir(filepath):
    return os.path.isdir(filepath)


def exists(filepath, adj=True):
    """
    Check whether the file or directory exists at the given path.

    Parameters:
    -----------
    filepath : str
        The path to the file or directory.

    Returns:
    --------
    bool
        True if the file or directory exists, False otherwise.
    """
    assert isinstance(filepath, str)
    if adj:
        filepath = adj_path(filepath)
    return os.path.exists(filepath)