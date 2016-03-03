# Author:	Atena Haghighattalab
## Created:	07/08/2014
# modified: 25.03.2015

import os
import PhotoScan


def main():

	global doc
	doc = PhotoScan.app.document

	app = QtGui.QApplication.instance()
	parent = app.activeWindow()
	
	#prompting for path to photos
	path_photos = PhotoScan.app.getExistingDirectory("Specify input photo folder:")
	path_psz = PhotoScan.app.getSaveFileName("Specify path for the PSZ project file:")
	path_export = PhotoScan.app.getExistingDirectory("Specify EXPORT folder:")
	
	#processing parameters
	accuracy = PhotoScan.Accuracy.HighAccuracy  #align photos accuracy
	preselection = PhotoScan.Preselection.ReferencePreselection
	keypoints = 40000 #align photos key point limit
	tiepoints = 10000 #align photos tie point limit
	source = PhotoScan.PointsSource.DensePoints #build mesh source
	surface = PhotoScan.SurfaceType.HeightField #build mesh surface type
	quality = PhotoScan.Quality.MediumQuality #build dense cloud quality
	filtering = PhotoScan.FilterMode.AggressiveFiltering #depth filtering
	interpolation = PhotoScan.Interpolation.EnabledInterpolation #build mesh interpolation 
	face_num = PhotoScan.FaceCount.HighFaceCount #build mesh polygon count
	mapping = PhotoScan.MappingMode.OrthophotoMapping #build texture mapping
	atlas_size = 8192
	blending = PhotoScan.BlendingMode.MosaicBlending #blending mode
	color_corr = False


	print("Script started")

	#creating new chunk
	doc.addChunk()
	chunk = doc.chunks[-1]
	chunk.label = "New Chunk"

	#loading images
	image_list = os.listdir(path_photos)
	photo_list = list()
	for photo in image_list:
		if ("jpg" or "jpeg" or "JPG" or "JPEG") in photo.lower():
			photo_list.append(path_photos + "\\" + photo)
	chunk.addPhotos(photo_list)

	#loading coordinates from EXIF and setting up the coordinate system
	chunk.loadReferenceExif()
	chunk.crs = PhotoScan.CoordinateSystem("EPSG::4326")
	proj = chunk.crs
	PhotoScan.app.update()

	#align photos
	chunk.matchPhotos(accuracy = accuracy, preselection = preselection, filter_mask = False, keypoint_limit = keypoints, tiepoint_limit = tiepoints)
	chunk.alignCameras()
	
	chunk.optimizeCameras()

	#building dense cloud
	PhotoScan.app.gpu_mask = 1  #GPU devices binary mask
	PhotoScan.app.cpu_cores_inactive = 2  #CPU cores inactive
	chunk.buildDenseCloud(quality = quality, filter = filtering)

	#building mesh
	chunk.buildModel(surface = surface, source = source, interpolation = interpolation, face_count = face_num)

	#build texture
	chunk.buildUV(mapping = mapping, count = 1)
	chunk.buildTexture(blending = blending , color_correction = color_corr, size = atlas_size)

	doc.save(path_psz)
	PhotoScan.app.update()

	###exporting DEM and Orthophoto

	#estimating effective ground resolution
	#recalculating WGS84 resolution from degrees into meters if export projection (proj) is WGS84
	if proj.authority == "EPSG::4326":
		coord = PhotoScan.Vector([0,0,0])
		cam_num = 0
		for camera in chunk.cameras:
			if not camera.transform:
				continue
			coord += proj.project(chunk.transform.matrix.mulp(camera.center))
			cam_num += 1
			
		crd = (1. / cam_num) * coord

		#longitude
		v1 = PhotoScan.Vector((crd[0], crd[1], 0) )
		v2 = PhotoScan.Vector((crd[0] + 0.001, crd[1], 0))
		vm1 = proj.unproject(v1)
		vm2 = proj.unproject(v2)
		res_x = (vm2 - vm1).norm() * 1000

		#latitude
		v2 = PhotoScan.Vector( (crd[0], crd[1] + 0.001, 0))
		vm2 = proj.unproject(v2)
		res_y = (vm2 - vm1).norm() * 1000
			
		d_x = d_x / res_x  
		d_y = d_y / res_y
		
	
	chunk.exportDem(path_export + "\\DEM.tif", format = "tif", projection = proj, dx = d_x, dy = d_y, nodata = -32767, crop_borders = True, write_kml = False, write_world = False)
	chunk.exportOrthophoto(path_export + "\\ortho.tif", format='tif', blending = blending, dx = d_x, dy = d_y, color_correction = color_corr, projection = proj, write_kml = False, write_world = False)

	print("Script finished")


PhotoScan.app.addMenuItem("Custom menu/Process 1", main)	