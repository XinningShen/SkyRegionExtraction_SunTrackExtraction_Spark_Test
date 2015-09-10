from __future__ import print_function
import io
import sys
import os

import cv2
import numpy as np

import ImageProcess
from pyspark import AccumulatorParam
from pyspark.storagelevel import StorageLevel
from pyspark import SparkFiles


HEIGHT = 640
WIDTH = 640


class MatrixAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return (np.zeros(value[0].shape[:2], np.uint8), np.zeros(value[1].shape[:2], np.uint8), 0)

    def addInPlace(self, v1, v2):
        x = np.add(v1[0], v2[0])
        y = np.add(v1[1], v2[1])
        z = v1[2] + v2[2]
        return (x, y, z)


if __name__ == "__main__":
    from pyspark import SparkContext, SparkConf

    # Set up Spark configuration. Four local cores!
    conf = SparkConf().setAppName("feature_extractor").setMaster("local[4]")
    # conf = SparkConf().setAppName("feature_extractor").setMaster("spark://192.168.1.41:7077")
    sc = SparkContext(conf = conf)

    # path = "file:///Users/Sidney/"

    try:
        image_seqfile_path_d1 = sys.argv[1]
        image_seqfile_path_d2 = sys.argv[2]
        image_seqfile_path_d3 = sys.argv[3]

        # image_seqfile_path_d1 = path + "04E6768323EE08-30_04-00_to_08-30_21-00.seq"
        # image_seqfile_path_d2 = path + "04E6768323EE08-31_04-00_to_08-31_21-00.seq"
        # image_seqfile_path_d3 = path + "04E6768323EE09-01_04-00_to_09-01_21-00.seq"
    except:
        print("Wrong Input!")

    # print("d1_path = ", image_seqfile_path_d1)
    # print("d2_path = ", image_seqfile_path_d2)
    # print("d3_path = ", image_seqfile_path_d3)

    # Read in Three days images (HDFS Sequence File) for processing
    images_d1 = sc.sequenceFile(path = image_seqfile_path_d1, minSplits = 4)
    images_d2 = sc.sequenceFile(path = image_seqfile_path_d2, minSplits = 4)
    images_d3 = sc.sequenceFile(path = image_seqfile_path_d3, minSplits = 4)

    # Extract images from Sequence File
    images_d1 = images_d1.map(ImageProcess.extract_image)
    images_d2 = images_d2.map(ImageProcess.extract_image)
    images_d3 = images_d3.map(ImageProcess.extract_image)

    # Save images RDD in memory for later use (get correlation coefficient)
    images_d1.persist(StorageLevel.MEMORY_ONLY)
    images_d2.persist(StorageLevel.MEMORY_ONLY)
    images_d3.persist(StorageLevel.MEMORY_ONLY)


    '''======================Sky Region Detection Part======================'''

    # Get new RDD: Sky Region for each image
    images_sky_region = images_d2.map(lambda x: ImageProcess.checkSkyRegion(x[1]))

    # Combine sky_region_color and sky_region_edge
    skyRegionPixelCount = (np.zeros((HEIGHT, WIDTH), np.uint8), np.zeros((HEIGHT, WIDTH), np.uint8), 0)
    pixel = sc.accumulator(skyRegionPixelCount, MatrixAccumulatorParam())
    images_sky_region.foreach(lambda x: pixel.add(x))
    
    images_count = pixel.value[2]

    # Generate sky-region-binary-image
    beta1 = 0.75
    beta2 = 0.5
    resultPic = ImageProcess.getSkyRegionMask(pixel.value[0], pixel.value[1], images_count * beta1, images_count * beta2)

    # Erode the sky-region-binary-image and mark three largest (if any) contour area.
    kernel = np.ones((5, 5), np.uint8)
    mask, cnts_new = ImageProcess.markDisjointSkyRegion(resultPic, kernel)

    # If detect more than one disjoint contour area, calculate correlation coefficient of each area.
    contour_num = len(cnts_new)
    if contour_num > 1:
        mask_b = sc.broadcast(mask)
        images_coefficient = images_d2.flatMap(lambda x: ImageProcess.get_R_Minus_B_Value(x[1], mask_b.value))
        coef_list = images_coefficient.groupByKey().mapValues(list).take(3)
        print (coef_list)

        ImageProcess.disjointRegionProcess(coef_list, contour_num, mask)

    # Get Final Sky Region Mask
    final_sky_region_mask = ImageProcess.generateFinalSkyRegion(mask, kernel, contour_num)

    cv2.imshow('final_sky_region', final_sky_region_mask)
    cv2.waitKey(0)

    sky_region_mask = sc.broadcast(final_sky_region_mask)



    '''====================Sun Track Extraction Part======================'''

    # Get three-days' filename list
    images_d1_filename_list = images_d1.map(lambda x: x[0]).collect()
    images_d2_filename_list = images_d2.map(lambda x: x[0]).collect()
    images_d3_filename_list = images_d3.map(lambda x: x[0]).collect()

    l1, l2_1, l2_3, l3 = ImageProcess.findMatchingFilenameBetweenTwoDay(images_d1_filename_list, images_d2_filename_list, images_d3_filename_list)

    print ("l1 length = " , len(l1))
    print ("l2_1 length = " , len(l2_1))
    print ('l2_3 length =', len(l2_3))
    print ('l3 length = ', len(l3))
    print ("l3 = ",  l3)

    # Get filtered file in each day
    images_d1_filter = images_d1.filter(lambda x: x[0] in l1)
    images_d21_filter = images_d2.filter(lambda x: x[0] in l2_1)
    images_d23_filter = images_d2.filter(lambda x: x[0] in l2_3)
    images_d3_filter = images_d3.filter(lambda x: x[0] in l3)

    # For each filtered file, apply Sun Detect Function
    images_d1_sunDetect = images_d1_filter.map(lambda x: ImageProcess.sunDetect(x[1], sky_region_mask.value))
    images_d21_sunDetect = images_d21_filter.map(lambda x: ImageProcess.sunDetect(x[1], sky_region_mask.value))
    images_d23_sunDetect = images_d23_filter.map(lambda x: ImageProcess.sunDetect(x[1], sky_region_mask.value))
    images_d3_sunDetect = images_d3_filter.map(lambda x: ImageProcess.sunDetect(x[1], sky_region_mask.value))

    # Sun Intersection Detect. (Note: coalesce to only ONE group to do this.)
    images_d12_intersect = images_d1_sunDetect.coalesce(1).zip(images_d21_sunDetect.coalesce(1)).filter(lambda x: x[0] is not None and x[1] is not None and ImageProcess.intersectionDetect(x[0], x[1])).collect()
    images_d23_intersect = images_d3_sunDetect.coalesce(1).zip(images_d23_sunDetect.coalesce(1)).filter(lambda x: x[0] is not None and x[1] is not None and ImageProcess.intersectionDetect(x[0], x[1])).collect()

    print (images_d12_intersect)
    print (images_d23_intersect)

    # Get intersection sun centroid list
    centroid_list = ImageProcess.getCentroidList(images_d12_intersect + images_d23_intersect)

    print (centroid_list)

    # Generate Sun Track
    try:
        theta, coeffs = ImageProcess.generalParabola(centroid_list)

        print ("theta = ", theta)
        print ("coeffs = ", coeffs)
    except:
        print ("Not enough sample to generate sun track")