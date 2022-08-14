REM Get OpenStreetMaps data for any size area using command line 

REM Two alternatives, --bbox if it is know the coordinates, or --area if it is know the areaID to retrieve. 

REM For the coordinates: go to https://www.openstreetmap.org search for the area you want to download, select the area in the Search Results, then click on Export on the top left side, then you will see the coordinates.

REM For the areaID: go to https://www.openstreetmap.org, search for the area you want to download, select the area in the Search Results, then copy the number after "relation/". 
REM Example: https://www.openstreetmap.org/relation/442324. In this case the areaID is 442324.

REM You must delete from the file the options you don't want, because commenting them will crash your inputs.

REM By areaID:
python "net_osmGet.py" ^
--osm-file "usti_orp.osm.xml" ^
--area 442324 ^
--time-out 5000
pause