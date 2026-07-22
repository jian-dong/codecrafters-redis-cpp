#include "test_support.hpp"

namespace {

using redis::CommandExecutor;
using redis::Database;
using redis::RespArray;
using redis::RespInteger;
using redis::RespWriter;

TEST(GeoTest, GeoaddReturnsAddedCountAndStoresScore) {
  Database database;
  CommandExecutor executor(database, false);

  redis::CommandResult result =
      executor.Execute({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"});
  ASSERT_TRUE((result.has_value())) << "GEOADD should succeed";
  ASSERT_TRUE(((*result).Is<RespInteger>())) << "GEOADD should return a RESP integer";
  ASSERT_TRUE(((*result).Get<RespInteger>().value == 1)) << "GEOADD should report one added location";
  ASSERT_TRUE((RespWriter::Write(*result) == ":1\r\n")) << "GEOADD should encode the added-location count as a RESP integer";

  result = executor.Execute({"ZRANGE", "places", "0", "-1"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE after GEOADD should succeed";
  ASSERT_TRUE(((*result).Is<RespArray>())) << "ZRANGE after GEOADD should return a RESP array";
  ASSERT_TRUE((RespBulkStrings((*result).Get<RespArray>()) ==
             std::vector<std::string>({"Paris"}))) << "GEOADD should store the location in the sorted set";

  result = executor.Execute({"ZSCORE", "places", "Paris"});
  ASSERT_TRUE((result.has_value())) << "ZSCORE after GEOADD should succeed";
  ASSERT_TRUE(((*result).Is<redis::RespBulkString>())) << "ZSCORE after GEOADD should return a RESP bulk string";
  ASSERT_TRUE(((*result).Get<redis::RespBulkString>().value == "3663832614298053")) << "GEOADD should store the correct geospatial score";
}

TEST(GeoTest, GeoaddRejectsInvalidCoordinates) {
  Database database;
  CommandExecutor executor(database, false);

  redis::CommandResult result =
      executor.Execute({"GEOADD", "places", "181", "0.3", "test2"});
  ASSERT_TRUE((!result.has_value())) << "GEOADD should reject invalid longitude";
  const std::string longitude_error =
      RespWriter::Error(CommandErrorMessage(result.error()));
  ASSERT_TRUE((longitude_error.starts_with("-ERR"))) << "invalid GEOADD longitude should encode as a RESP error";
  ASSERT_TRUE((longitude_error.find("longitude") != std::string::npos)) << "invalid GEOADD longitude error should mention longitude";

  result = executor.Execute({"GEOADD", "places", "180", "90", "test1"});
  ASSERT_TRUE((!result.has_value())) << "GEOADD should reject invalid latitude";
  const std::string latitude_error =
      RespWriter::Error(CommandErrorMessage(result.error()));
  ASSERT_TRUE((latitude_error.starts_with("-ERR"))) << "invalid GEOADD latitude should encode as a RESP error";
  ASSERT_TRUE((latitude_error.find("latitude") != std::string::npos)) << "invalid GEOADD latitude error should mention latitude";
}

TEST(GeoTest, GeoposReturnsCoordinatesOrNil) {
  Database database;
  CommandExecutor executor(database, false);

  ASSERT_TRUE((executor.Execute({"GEOADD", "location_key", "-0.0884948", "51.506479", "London"}).has_value())) << "setup GEOADD London should succeed";
  ASSERT_TRUE((executor.Execute({"GEOADD", "location_key", "11.5030378", "48.164271", "Munich"}).has_value())) << "setup GEOADD Munich should succeed";

  redis::CommandResult result =
      executor.Execute({"GEOPOS", "location_key", "London", "Munich"});
  ASSERT_TRUE((result.has_value())) << "GEOPOS existing members should succeed";
  ASSERT_TRUE(((*result).Is<redis::RespArray>())) << "GEOPOS should return a structured RESP array";
  ASSERT_TRUE((RespWriter::Write(*result) ==
          "*2\r\n*2\r\n$20\r\n-0.08849412202835083\r\n$18\r\n51.506478141399342\r\n*2\r\n$18\r\n11.503036916255951\r\n$18\r\n48.164270862329779\r\n")) << "GEOPOS should decode existing members into coordinate pairs";

  result = executor.Execute({"GEOPOS", "location_key", "missing_location"});
  ASSERT_TRUE((result.has_value())) << "GEOPOS missing member should succeed";
  ASSERT_TRUE(((*result).Is<redis::RespArray>())) << "GEOPOS missing member should return a structured RESP array";
  ASSERT_TRUE((RespWriter::Write(*result) == "*1\r\n*-1\r\n")) << "GEOPOS missing member should encode as a null array element";

  result = executor.Execute({"GEOPOS", "missing_key", "London", "Munich"});
  ASSERT_TRUE((result.has_value())) << "GEOPOS missing key should succeed";
  ASSERT_TRUE(((*result).Is<redis::RespArray>())) << "GEOPOS missing key should return a structured RESP array";
  ASSERT_TRUE((RespWriter::Write(*result) == "*2\r\n*-1\r\n*-1\r\n")) << "GEOPOS missing key should encode each requested member as null";
}

TEST(GeoTest, GeoposDecodesCoordinatesFromZsetScores) {
  Database database;
  CommandExecutor executor(database, false);

  ASSERT_TRUE((executor.Execute({"ZADD", "location_key", "3663832614298053", "Foo"}).has_value())) << "setup ZADD Foo should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "location_key", "3876464048901851", "Bar"}).has_value())) << "setup ZADD Bar should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "location_key", "3468915414364476", "Baz"}).has_value())) << "setup ZADD Baz should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "location_key", "3781709020344510", "Caz"}).has_value())) << "setup ZADD Caz should succeed";

  redis::CommandResult result = executor.Execute({"GEOPOS", "location_key", "Foo"});
  ASSERT_TRUE((result.has_value())) << "GEOPOS Foo should succeed";
  ASSERT_TRUE(((*result).Is<redis::RespArray>())) << "GEOPOS Foo should return a structured RESP array";
  ASSERT_TRUE((RespWriter::Write(*result) ==
          "*1\r\n*2\r\n$18\r\n2.2944715619087219\r\n$17\r\n48.85846255040142\r\n")) << "GEOPOS should decode coordinates from sorted-set scores";
}

TEST(GeoTest, GeodistReturnsDistanceBetweenLocations) {
  Database database;
  CommandExecutor executor(database, false);

  ASSERT_TRUE((executor.Execute({"GEOADD", "places", "11.5030378", "48.164271", "Munich"}).has_value())) << "setup GEOADD Munich should succeed";
  ASSERT_TRUE((executor.Execute({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"}).has_value())) << "setup GEOADD Paris should succeed";

  redis::CommandResult result = executor.Execute({"GEODIST", "places", "Munich", "Paris"});
  ASSERT_TRUE((result.has_value())) << "GEODIST should succeed";
  ASSERT_TRUE(((*result).Is<redis::RespBulkString>())) << "GEODIST should return a RESP bulk string";
  ASSERT_TRUE(((*result).Get<redis::RespBulkString>().value == "682477.7582")) << "GEODIST should return the distance in meters";
  ASSERT_TRUE((RespWriter::Write(*result) == "$11\r\n682477.7582\r\n")) << "GEODIST should encode the distance as a RESP bulk string";
}

TEST(GeoTest, GeosearchReturnsMembersWithinRadius) {
  Database database;
  CommandExecutor executor(database, false);

  ASSERT_TRUE((executor.Execute({"GEOADD", "places", "11.5030378", "48.164271", "Munich"}).has_value())) << "setup GEOADD Munich should succeed";
  ASSERT_TRUE((executor.Execute({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"}).has_value())) << "setup GEOADD Paris should succeed";
  ASSERT_TRUE((executor.Execute({"GEOADD", "places", "-0.0884948", "51.506479", "London"}).has_value())) << "setup GEOADD London should succeed";

  redis::CommandResult result =
      executor.Execute({"GEOSEARCH", "places", "FROMLONLAT", "2", "48",
                        "BYRADIUS", "100000", "m"});
  ASSERT_TRUE((result.has_value())) << "GEOSEARCH within 100km should succeed";
  ASSERT_TRUE(((*result).Is<RespArray>())) << "GEOSEARCH should return a RESP array";
  ASSERT_TRUE((RespBulkStrings((*result).Get<RespArray>()) ==
             std::vector<std::string>({"Paris"}))) << "GEOSEARCH should return only Paris within 100km of 2,48";

  result = executor.Execute({"GEOSEARCH", "places", "FROMLONLAT", "2", "48",
                             "BYRADIUS", "500000", "m"});
  ASSERT_TRUE((result.has_value())) << "GEOSEARCH within 500km should succeed";
  ASSERT_TRUE(((*result).Is<RespArray>())) << "GEOSEARCH multi-match response should be a RESP array";
  std::vector<std::string> matches =
      RespBulkStrings((*result).Get<RespArray>());
  std::sort(matches.begin(), matches.end());
  ASSERT_TRUE((matches == std::vector<std::string>({"London", "Paris"}))) << "GEOSEARCH should return Paris and London within 500km of 2,48";

  result = executor.Execute({"GEOSEARCH", "places", "FROMLONLAT", "11", "50",
                             "BYRADIUS", "300000", "m"});
  ASSERT_TRUE((result.has_value())) << "GEOSEARCH near Munich should succeed";
  ASSERT_TRUE(((*result).Is<RespArray>())) << "GEOSEARCH near Munich should return a RESP array";
  ASSERT_TRUE((RespBulkStrings((*result).Get<RespArray>()) ==
             std::vector<std::string>({"Munich"}))) << "GEOSEARCH should return Munich within 300km of 11,50";

  result = executor.Execute({"GEOSEARCH", "missing_key", "FROMLONLAT", "2",
                             "48", "BYRADIUS", "100000", "m"});
  ASSERT_TRUE((result.has_value())) << "GEOSEARCH missing key should succeed";
  ASSERT_TRUE(((*result).Is<RespArray>())) << "GEOSEARCH missing key should return a RESP array";
  ASSERT_TRUE(((*result).Get<RespArray>().values.empty())) << "GEOSEARCH missing key should return an empty array";
}

}  // namespace
