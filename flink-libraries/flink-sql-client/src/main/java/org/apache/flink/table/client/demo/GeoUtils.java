/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.demo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * GeoUtils provides utility methods to deal with locations for the data streaming exercises.
 */
public class GeoUtils {

	// geo boundaries of the area of NYC
	public static double lonEast = -73.7;
	public static double lonWest = -74.05;
	public static double latNorth = 41.0;
	public static double latSouth = 40.5;

	// area width and height
	public static double lonWidth = 74.05 - 73.7;
	public static double latHeight = 41.0 - 40.5;

	// delta step to create artificial grid overlay of NYC
	public static double deltaLon = 0.0014;
	public static double deltaLat = 0.00125;

	// ( |lonWest| - |lonEast| ) / deltaLat
	public static int noGridX = 250;
	// ( latNorth - latSouth ) / deltaLon
	public static int noGridY = 400;

	public static float degLen = 110.25f;

	/**
	 * Checks if a location specified by longitude and latitude values is
	 * within the geo boundaries of New York City.
	 *
	 * @param lon longitude of the location to check
	 * @param lat latitude of the location to check
	 *
	 * @return true if the location is within NYC boundaries, otherwise false.
	 */
	public static boolean isInNYC(float lon, float lat) {

		return !(lon > lonEast || lon < lonWest) &&
			!(lat > latNorth || lat < latSouth);
	}

	/**
	 * Maps a location specified by latitude and longitude values to a cell of a
	 * grid covering the area of NYC.
	 * The grid cells are roughly 100 x 100 m and sequentially number from north-west
	 * to south-east starting by zero.
	 *
	 * @param lon longitude of the location to map
	 * @param lat latitude of the location to map
	 *
	 * @return id of mapped grid cell.
	 */
	public static int mapToGridCell(float lon, float lat) {
		int xIndex = (int) Math.floor((Math.abs(lonWest) - Math.abs(lon)) / deltaLon);
		int yIndex = (int) Math.floor((latNorth - lat) / deltaLat);

		return xIndex + (yIndex * noGridX);
	}

	/**
	 * Maps the direct path between two locations specified by longitude and latitude to a list of
	 * cells of a grid covering the area of NYC.
	 * The grid cells are roughly 100 x 100 m and sequentially number from north-west
	 * to south-east starting by zero.
	 *
	 * @param lon1 longitude of the first location
	 * @param lat1 latitude of the first location
	 * @param lon2 longitude of the second location
	 * @param lat2 latitude of the second location
	 *
	 * @return A list of cell ids
	 */
	public static List<Integer> mapToGridCellsOnWay(float lon1, float lat1, float lon2, float lat2) {

		int x1 = (int) Math.floor((Math.abs(lonWest) - Math.abs(lon1)) / deltaLon);
		int y1 = (int) Math.floor((latNorth - lat1) / deltaLat);

		int x2 = (int) Math.floor((Math.abs(lonWest) - Math.abs(lon2)) / deltaLon);
		int y2 = (int) Math.floor((latNorth - lat2) / deltaLat);

		int startX, startY, endX, endY;
		if (x1 <= x2) {
			startX = x1;
			startY = y1;
			endX = x2;
			endY = y2;
		}
		else {
			startX = x2;
			startY = y2;
			endX = x1;
			endY = y1;
		}

		double slope = (endY - startY) / ((endX - startX) + 0.00000001);

		int curX = startX;
		int curY = startY;

		ArrayList<Integer> cellIds = new ArrayList<>(64);
		cellIds.add(curX + (curY * noGridX));

		while (curX < endX || curY != endY) {

			if (slope > 0) {
				double y = (curX - startX + 0.5) * slope + startY - 0.5;

				if (y > curY - 0.05 && y < curY + 0.05) {
					curX++;
					curY++;
				}
				else if (y < curY) {
					curX++;
				}
				else {
					curY++;
				}
			}
			else {
				double y = (curX - startX + 0.5) * slope + startY + 0.5;

				if (y > curY - 0.05 && y < curY + 0.05) {
					curX++;
					curY--;
				}
				if (y > curY) {
					curX++;
				}
				else {
					curY--;
				}

			}

			cellIds.add(curX + (curY * noGridX));
		}

		return cellIds;
	}

	/**
	 * Returns the longitude of the center of a grid cell.
	 *
	 * @param gridCellId The grid cell.
	 *
	 * @return The longitude value of the cell's center.
	 */
	public static float getGridCellCenterLon(int gridCellId) {

		int xIndex = gridCellId % noGridX;

		return (float) (Math.abs(lonWest) - (xIndex * deltaLon) - (deltaLon / 2)) * -1.0f;
	}

	/**
	 * Returns the latitude of the center of a grid cell.
	 *
	 * @param gridCellId The grid cell.
	 *
	 * @return The latitude value of the cell's center.
	 */
	public static float getGridCellCenterLat(int gridCellId) {

		int xIndex = gridCellId % noGridX;
		int yIndex = (gridCellId - xIndex) / noGridX;

		return (float) (latNorth - (yIndex * deltaLat) - (deltaLat / 2));

	}

	/**
	 * Returns a random longitude within the NYC area.
	 *
	 * @param rand A random number generator.
	 * @return A random longitude value within the NYC area.
	 */
	public static float getRandomNYCLon(Random rand) {
		return (float) (lonEast - (lonWidth * rand.nextFloat()));
	}

	/**
	 * Returns a random latitude within the NYC area.
	 *
	 * @param rand A random number generator.
	 * @return A random latitude value within the NYC area.
	 */
	public static float getRandomNYCLat(Random rand) {
		return (float) (latSouth + (latHeight * rand.nextFloat()));
	}

	/**
	 * Returns the Euclidean distance between two locations specified as lon/lat pairs.
	 *
	 * @param lon1 Longitude of first location
	 * @param lat1 Latitude of first location
	 * @param lon2 Longitude of second location
	 * @param lat2 Latitude of second location
	 * @return The Euclidean distance between the specified locations.
	 */
	public static double getEuclideanDistance(float lon1, float lat1, float lon2, float lat2) {
		double x = lat1 - lat2;
		double y = (lon1 - lon2) * Math.cos(lat2);
		return (degLen * Math.sqrt(x * x + y * y));
	}

	/**
	 * Returns the angle in degrees between the vector from the start to the destination
	 * and the x-axis on which the start is located.
	 *
	 * <p>The angle describes in which direction the destination is located from the start, i.e.,
	 * 0° -> East, 90° -> South, 180° -> West, 270° -> North
	 *
	 * @param startLon longitude of start location
	 * @param startLat latitude of start location
	 * @param destLon longitude of destination
	 * @param destLat latitude of destination
	 * @return The direction from start to destination location
	 */
	public static int getDirectionAngle(
		float startLon, float startLat, float destLon, float destLat) {

		double x = destLat - startLat;
		double y = (destLon - startLon) * Math.cos(startLat);
		int degrees = (int) Math.toDegrees(Math.atan2(x, y)) + 179;

		return degrees;
	}

	/**
	 * Table API / SQL Scalar UDF to check if a coordinate is in NYC.
	 */
	public static class IsInNYC extends ScalarFunction {
		public boolean eval(float lon, float lat) {
			return isInNYC(lon, lat);
		}
	}

	/**
	 * Table API / SQL Scalar UDF to convert a lon/lat pair into a cell ID.
	 */
	public static class ToCellId extends ScalarFunction {
		public int eval(float lon, float lat) {
			return GeoUtils.mapToGridCell(lon, lat);
		}
	}

	/**
	 * Table API / SQL Scalar UDF to convert a cell ID into a lon/lat pair.
	 */
	public static class ToCoords extends ScalarFunction {
		public Tuple2<Float, Float> eval(int cellId) {
			return Tuple2.of(
				GeoUtils.getGridCellCenterLon(cellId),
				GeoUtils.getGridCellCenterLat(cellId)
			);
		}

		@Override
		public TypeInformation getResultType(Class[] signature) {
			return new TupleTypeInfo<>(Types.FLOAT, Types.FLOAT);
		}

	}

}
