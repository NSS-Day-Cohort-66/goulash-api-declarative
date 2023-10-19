import json
from nss_handler import status
from repository import db_get_single, db_get_all, db_delete, db_update, db_create
from services import expand_hauler


class HaulerView:
    def get(self, handler, query_params, pk):
        sql = "SELECT h.id, h.name, h.dock_id"
        if pk != 0:
            if "_expand" in query_params:
                sql += ", d.id dockId, d.location, d.capacity FROM Hauler h JOIN Dock d ON d.id = h.dock_id WHERE h.id = ?"
                query_results = db_get_single(sql, pk)
                serialized_hauler = json.dumps(expand_hauler(query_results))
            else:
                sql += " FROM Hauler h WHERE h.id = ?"
                query_results = db_get_single(sql, pk)
                serialized_hauler = json.dumps(dict(query_results))

            return handler.response(serialized_hauler, status.HTTP_200_SUCCESS.value)
        else:
            if "_expand" in query_params:
                sql += ", d.id dockId, d.location, d.capacity FROM Hauler h JOIN Dock d ON d.id = h.dock_id"
                query_results = db_get_all(sql)
                haulers = [expand_hauler(row) for row in query_results]
            else:
                sql += " FROM Hauler h"
                query_results = db_get_all(sql)
                haulers = [dict(row) for row in query_results]
            serialized_haulers = json.dumps(haulers)

            return handler.response(serialized_haulers, status.HTTP_200_SUCCESS.value)

    def delete(self, handler, pk):
        number_of_rows_deleted = db_delete("DELETE FROM Hauler WHERE id = ?", pk)

        if number_of_rows_deleted > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response(
                "", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value
            )

    def update(self, handler, hauler_data, pk):
        sql = """
        UPDATE Hauler
        SET
            name = ?,
            dock_id = ?
        WHERE id = ?
        """
        number_of_rows_updated = db_update(
            sql, (hauler_data["name"], hauler_data["dock_id"], pk)
        )

        if number_of_rows_updated > 0:
            return handler.response("", status.HTTP_204_SUCCESS_NO_RESPONSE_BODY.value)
        else:
            return handler.response(
                "", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value
            )

    def create(self, handler, hauler_data):
        sql = """
        INSERT INTO Hauler (name, dock_id)
        VALUES (?, ?)
        """
        last_row_id = db_create(sql, (hauler_data["name"], hauler_data["dock_id"]))

        if last_row_id > 0:
            hauler = {
                "id": last_row_id,
                "name": hauler_data["name"],
                "dock_id": hauler_data["dock_id"],
            }
            return handler.response(
                json.dumps(hauler), status.HTTP_201_SUCCESS_CREATED.value
            )
        else:
            return handler.response(
                "", status.HTTP_404_CLIENT_ERROR_RESOURCE_NOT_FOUND.value
            )
