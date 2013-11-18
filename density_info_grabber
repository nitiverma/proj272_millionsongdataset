###########################################################
###                   Infobright 2011                   ###
###            Developed by: Infobright Intern Team     ###
###                 Author: Infobright Intern Team      ###
###                     Version 1.0                     ###
###                                                     ###
### v1.0: Infobright Intern Team                        ###
###                                                     ###
### The MIT License                                     ###
###                                                     ###
### Copyright (c) 2011 Infobright Inc.                  ###
###                                                     ###
### Permission is hereby granted, free of charge, to    ###
### any person obtaining a copy of this software and    ###
### associated documentation files (the "Software"), to ###
### deal in the Software without restriction, including ###
### without limitation the rights to use, copy, modify, ###
### merge, publish, distribute, sublicense, and/or sell ###
### copies of the Software, and to permit persons to    ###
### whom the Software is furnished to do so, subject to ###
### the following conditions:                           ###
###                                                     ###
### The above copyright notice and this permission      ###
### notice shall be included in all copies or           ###
### substantial portions of the Software.               ###
###                                                     ###
### THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY  ###
### OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT  ###
### LIMITED TO THE WARRANTIES OF MERCHANTABILITY,       ###
### FITNESS FOR A PARTICULAR PURPOSE AND NON-           ###
### INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR      ###
### COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES  ###
### OR OTHER LIABILITY, WHETHER IN AN ACTION OF         ###
### CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF   ###
### OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR    ###
### OTHER DEALINGS IN THE SOFTWARE.                     ###
##########################################################h
# Description                                            #h
#                                                        #h
#   This script grabs data from the MillionSong tables   #h
#   and generates a flat file that contains              #h
#   data that will be used to populate the               #h
#   Metadata table.                                      #h
#                                                        #h
# Assumption(s):                                         #h
# - numpy, tables, numexpr and python are installed      #h
# - installed with default location for server script    #h
#                                                        #h
# Caveats:                                               #h
# - tested only on 32/64 bit CentOS and Ubuntu           #h
#                                                        #h
##########################################################h
# Usage                                                  #h
#                                                        #h
# <path>/python metadata_data_grabber.py                 #h
#                                                        #h
##########################################################h

import os
import glob
import csv
import hdf5_getters
import numpy
import array
import genre_dict
import string
import time
import pydoc



def get_avg(array):
	""" Return the average of an array."""
        size=len(array)
	if size != 0:
		average= numpy.average(array)
		return average	
	else:
		return 0

def get_max(array):
	"""Return the maximum number in an array."""
	size=len(array)
	if size != 0:
		max=numpy.amax(array)
		return max
	else:
		return 0

def get_min(array):
	"""Return the minimum number in an array."""
	size=len(array)
	if size != 0:
		min=numpy.amin(array)
		return min
	else:
		return 0
	

def get_count(array):
	"""Return the number of elements in an array."""
	size=len(array)
	if size != 0:
		count= len(array)
		return count
	else:
		return 0


def get_sum(array):
	"""Return the total sum of the elements in the array."""
	size=len(array)
	if size != 0:
		summy = numpy.sum(array)
		return summy
	else:
		return 0


def get_stddev(array):
	"""Return the standard deviation of the elements in the array."""
	size=len(array)
	if size != 0:
		stddev = numpy.std(array)
		return stddev
	else:
		return 0

		

def get_genre_indexes(array):
	""" Return the biggest 10 frequencies in the array."""
	ind = numpy.argsort(array)
	rev = ind[::-1]
	indexes=rev[0:10]
	return indexes

def get_genre(array,index):
	""" Returns the element that corresponds to that index in the array."""
	return array[index]


def is_number(s):
	"""Returns True/False depending if the input is a number or not."""
	try:
		float(s)
		return True
	except ValueError:
		return False

def genre_columns(array):
	"""Return the genre_array, each element of the array corresponds to a genre. Therefore it will contain a 1 if that genre corrsponds to the song and 0 otherwise. """
	genre_array=['0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'
,'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0',
'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0',
'0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0',
'0','0','0','0','0','0']
	if array == -1:  #genre was not found in dictionary
		return genre_array
	else:
		for index in range(len(array)):
			array[index]=array[index]-1

		for i in range(len(genre_array)):
			for index in array:
				if i == index:
					genre_array[i]='1'
		return genre_array



         #files = glob.glob(os.path.join(root,'TRAIIUE128E07826EC.h5'))
	 #files = glob.glob(os.path.join(root,'*'+ext))


def data_to_flat_file(basedir,ext='.h5') :
    """This function extract the information from the tables and creates the flat file."""	
    count = 0;	#song counter
    list_to_write= []
    row_to_write = ""
    writer = csv.writer(open("metadata_wholeA.csv", "wb"))
    for root, dirs, files in os.walk(basedir):
	files = glob.glob(os.path.join(root,'*'+ext))
        for f in files:
	    print f	#the name of the file
            h5 = hdf5_getters.open_h5_file_read(f)
	    title = hdf5_getters.get_title(h5) 
	    title= title.replace('"','') 
	    comma=title.find(',')	#eliminating commas in the title
	    if	comma != -1:
		    print title
		    time.sleep(1)
	    album = hdf5_getters.get_release(h5)
	    album= album.replace('"','')	#eliminating commas in the album	
	    comma=album.find(',')
	    if	comma != -1:
		    print album
		    time.sleep(1)
	    artist_name = hdf5_getters.get_artist_name(h5)
	    comma=artist_name.find(',')
	    if	comma != -1:
		    print artist_name
		    time.sleep(1)
	    artist_name= artist_name.replace('"','')	#eliminating double quotes
	    duration = hdf5_getters.get_duration(h5)
	    samp_rt = hdf5_getters.get_analysis_sample_rate(h5)
	    artist_7digitalid = hdf5_getters.get_artist_7digitalid(h5)
	    artist_fam = hdf5_getters.get_artist_familiarity(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(artist_fam) == True:
	            artist_fam=-1
	    artist_hotness= hdf5_getters.get_artist_hotttnesss(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(artist_hotness) == True:
	            artist_hotness=-1
	    artist_id = hdf5_getters.get_artist_id(h5)
	    artist_lat = hdf5_getters.get_artist_latitude(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(artist_lat) == True:
	            artist_lat=-1
	    artist_loc = hdf5_getters.get_artist_location(h5)
		#checks artist_loc to see if it is a hyperlink if it is set as empty string
	    artist_loc = artist_loc.replace(",", "\,");
	    if artist_loc.startswith("<a"):
                artist_loc = ""
	    if len(artist_loc) > 100:
                artist_loc = ""
	    artist_lon = hdf5_getters.get_artist_longitude(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(artist_lon) == True:
	            artist_lon=-1
	    artist_mbid = hdf5_getters.get_artist_mbid(h5)
	    artist_pmid = hdf5_getters.get_artist_playmeid(h5)
	    audio_md5 = hdf5_getters.get_audio_md5(h5)
	    danceability = hdf5_getters.get_danceability(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(danceability) == True:
	            danceability=-1
	    end_fade_in =hdf5_getters.get_end_of_fade_in(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(end_fade_in) == True:
	            end_fade_in=-1
	    energy = hdf5_getters.get_energy(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(energy) == True:
	            energy=-1
            song_key = hdf5_getters.get_key(h5)
	    key_c = hdf5_getters.get_key_confidence(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(key_c) == True:
	            key_c=-1
	    loudness = hdf5_getters.get_loudness(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(loudness) == True:
	            loudness=-1
	    mode = hdf5_getters.get_mode(h5)
	    mode_conf = hdf5_getters.get_mode_confidence(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(mode_conf) == True:
	            mode_conf=-1
	    release_7digitalid = hdf5_getters.get_release_7digitalid(h5)
	    song_hot = hdf5_getters.get_song_hotttnesss(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(song_hot) == True:
	            song_hot=-1
	    song_id = hdf5_getters.get_song_id(h5)
	    start_fade_out = hdf5_getters.get_start_of_fade_out(h5)
	    tempo = hdf5_getters.get_tempo(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(tempo) == True:
	            tempo=-1
	    time_sig = hdf5_getters.get_time_signature(h5)
	    time_sig_c = hdf5_getters.get_time_signature_confidence(h5)
	    #checking if we get a "nan" if we do we change it to -1
	    if numpy.isnan(time_sig_c) == True:
	            time_sig_c=-1
	    track_id = hdf5_getters.get_track_id(h5)
	    track_7digitalid = hdf5_getters.get_track_7digitalid(h5)
	    year = hdf5_getters.get_year(h5)
	    bars_c = hdf5_getters.get_bars_confidence(h5)
	    bars_c_avg= get_avg(bars_c)
	    bars_c_max= get_max(bars_c)
	    bars_c_min = get_min(bars_c)
	    bars_c_stddev= get_stddev(bars_c)
	    bars_c_count = get_count(bars_c)
	    bars_c_sum = get_sum(bars_c)
	    bars_start = hdf5_getters.get_bars_start(h5)
	    bars_start_avg = get_avg(bars_start)
	    bars_start_max= get_max(bars_start)
	    bars_start_min = get_min(bars_start)
	    bars_start_stddev= get_stddev(bars_start)
	    bars_start_count = get_count(bars_start)
	    bars_start_sum = get_sum(bars_start)
            beats_c = hdf5_getters.get_beats_confidence(h5)
            beats_c_avg= get_avg(beats_c)
	    beats_c_max= get_max(beats_c)
	    beats_c_min = get_min(beats_c)
	    beats_c_stddev= get_stddev(beats_c)
	    beats_c_count = get_count(beats_c)
	    beats_c_sum = get_sum(beats_c)
            beats_start = hdf5_getters.get_beats_start(h5)
 	    beats_start_avg = get_avg(beats_start)
	    beats_start_max= get_max(beats_start)
	    beats_start_min = get_min(beats_start)
	    beats_start_stddev= get_stddev(beats_start)
	    beats_start_count = get_count(beats_start)
	    beats_start_sum = get_sum(beats_start)
	    sec_c = hdf5_getters.get_sections_confidence(h5)
            sec_c_avg= get_avg(sec_c)
	    sec_c_max= get_max(sec_c)
	    sec_c_min = get_min(sec_c)
	    sec_c_stddev= get_stddev(sec_c)
	    sec_c_count = get_count(sec_c)
	    sec_c_sum = get_sum(sec_c)
	    sec_start = hdf5_getters.get_sections_start(h5)
            sec_start_avg = get_avg(sec_start)
	    sec_start_max= get_max(sec_start)
	    sec_start_min = get_min(sec_start)
	    sec_start_stddev= get_stddev(sec_start)
	    sec_start_count = get_count(sec_start)
	    sec_start_sum = get_sum(sec_start)
	    seg_c = hdf5_getters.get_segments_confidence(h5)
	    seg_c_avg= get_avg(seg_c)
	    seg_c_max= get_max(seg_c)
	    seg_c_min = get_min(seg_c)
	    seg_c_stddev= get_stddev(seg_c)
	    seg_c_count = get_count(seg_c)
	    seg_c_sum = get_sum(seg_c)
            seg_loud_max = hdf5_getters.get_segments_loudness_max(h5)
            seg_loud_max_avg= get_avg(seg_loud_max)
	    seg_loud_max_max= get_max(seg_loud_max)
	    seg_loud_max_min = get_min(seg_loud_max)
	    seg_loud_max_stddev= get_stddev(seg_loud_max)
	    seg_loud_max_count = get_count(seg_loud_max)
	    seg_loud_max_sum = get_sum(seg_loud_max)
	    seg_loud_max_time = hdf5_getters.get_segments_loudness_max_time(h5)
	    seg_loud_max_time_avg= get_avg(seg_loud_max_time)
	    seg_loud_max_time_max= get_max(seg_loud_max_time)
	    seg_loud_max_time_min = get_min(seg_loud_max_time)
	    seg_loud_max_time_stddev= get_stddev(seg_loud_max_time)
	    seg_loud_max_time_count = get_count(seg_loud_max_time)
	    seg_loud_max_time_sum = get_sum(seg_loud_max_time)
	    seg_loud_start = hdf5_getters.get_segments_loudness_start(h5)
	    seg_loud_start_avg= get_avg(seg_loud_start)
	    seg_loud_start_max= get_max(seg_loud_start)
	    seg_loud_start_min = get_min(seg_loud_start)
	    seg_loud_start_stddev= get_stddev(seg_loud_start)
	    seg_loud_start_count = get_count(seg_loud_start)
	    seg_loud_start_sum = get_sum(seg_loud_start)					      
	    seg_pitch = hdf5_getters.get_segments_pitches(h5)
	    pitch_size = len(seg_pitch)
	    seg_start = hdf5_getters.get_segments_start(h5)
	    seg_start_avg= get_avg(seg_start)
	    seg_start_max= get_max(seg_start)
	    seg_start_min = get_min(seg_start)
	    seg_start_stddev= get_stddev(seg_start)
	    seg_start_count = get_count(seg_start)
	    seg_start_sum = get_sum(seg_start)
	    seg_timbre = hdf5_getters.get_segments_timbre(h5)
	    tatms_c = hdf5_getters.get_tatums_confidence(h5)
	    tatms_c_avg= get_avg(tatms_c)
	    tatms_c_max= get_max(tatms_c)
	    tatms_c_min = get_min(tatms_c)
	    tatms_c_stddev= get_stddev(tatms_c)
	    tatms_c_count = get_count(tatms_c)
	    tatms_c_sum = get_sum(tatms_c)
	    tatms_start = hdf5_getters.get_tatums_start(h5)
	    tatms_start_avg= get_avg(tatms_start)
	    tatms_start_max= get_max(tatms_start)
	    tatms_start_min = get_min(tatms_start)
	    tatms_start_stddev= get_stddev(tatms_start)
	    tatms_start_count = get_count(tatms_start)
	    tatms_start_sum = get_sum(tatms_start)
	
	    #Getting the genres
	    genre_set = 0    #flag to see if the genre has been set or not
	    art_trm = hdf5_getters.get_artist_terms(h5)
	    trm_freq = hdf5_getters.get_artist_terms_freq(h5)
	    trn_wght = hdf5_getters.get_artist_terms_weight(h5)
	    a_mb_tags = hdf5_getters.get_artist_mbtags(h5)
	    genre_indexes=get_genre_indexes(trm_freq) #index of the highest freq
	    final_genre=[]
	    genres_so_far=[]
	    for i in range(len(genre_indexes)):
		    genre_tmp=get_genre(art_trm,genre_indexes[i])   #genre that corresponds to the highest freq
		    genres_so_far=genre_dict.get_genre_in_dict(genre_tmp) #getting the genre from the dictionary
		    if len(genres_so_far) != 0:
			    for i in genres_so_far:
				final_genre.append(i)
				genre_set=1				#genre was found in dictionary
				  
		
	    
	    if genre_set == 1:
		    col_num=[]
		   
		    for genre in final_genre:
			    column=int(genre)				#getting the column number of the genre
			    col_num.append(column)

		    genre_array=genre_columns(col_num)	         #genre array
 	    else:
		    genre_array=genre_columns(-1)		#the genre was not found in the dictionary

	    transpose_pitch= seg_pitch.transpose() #this is to tranpose the matrix,so we can have 12 rows
	    #arrays containing the aggregate values of the 12 rows
	    seg_pitch_avg=[]
	    seg_pitch_max=[]
	    seg_pitch_min=[]
            seg_pitch_stddev=[]
            seg_pitch_count=[]
	    seg_pitch_sum=[]
            i=0
	    #Getting the aggregate values in the pitches array
	    for row in transpose_pitch:
		   seg_pitch_avg.append(get_avg(row))
		   seg_pitch_max.append(get_max(row))
	           seg_pitch_min.append(get_min(row))
		   seg_pitch_stddev.append(get_stddev(row))
		   seg_pitch_count.append(get_count(row))
                   seg_pitch_sum.append(get_sum(row))
		   i=i+1

	    #extracting information from the timbre array 
            transpose_timbre = seg_pitch.transpose() #tranposing matrix, to have 12 rows
	    #arrays containing the aggregate values of the 12 rows
	    seg_timbre_avg=[]
	    seg_timbre_max=[]
	    seg_timbre_min=[]
            seg_timbre_stddev=[]
            seg_timbre_count=[]
	    seg_timbre_sum=[]
            i=0
	    for row in transpose_timbre:
		   seg_timbre_avg.append(get_avg(row))
		   seg_timbre_max.append(get_max(row))
	           seg_timbre_min.append(get_min(row))
		   seg_timbre_stddev.append(get_stddev(row))
		   seg_timbre_count.append(get_count(row))
                   seg_timbre_sum.append(get_sum(row))
		   i=i+1
		


		#Writing to the flat file
            writer.writerow([title,album,artist_name,year,duration,seg_start_count, tempo])

	    h5.close()
	    count=count+1;
	    print count;
		
#change the directory to the one you want
data_to_flat_file("HDF5_wholeA");
