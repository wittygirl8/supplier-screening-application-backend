import os
from typing import Dict
from fastapi import Depends, logger, HTTPException, status
from neo4j import AsyncGraphDatabase
from sqlalchemy import and_, func, or_, tuple_,  update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models import STATUS, Base, FinalStatus, FinalValidatedStatus, OribisMatchStatus
from app.api import deps
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import aliased
from datetime import datetime, timedelta
from app.schemas.logger import logger

async def get_dynamic_ens_data(
    table_name: str, 
    required_columns: list, 
    ens_id: str = "", 
    session_id: str = "", 
    session=None, 
    **kwargs
):
    try:
        extra_filters = kwargs.get('extra_filters', {})

        # Validate if table exists
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table '{table_name}' does not exist in the database schema."
            )

        # Prepare columns to select
        columns_to_select = [getattr(table_class.c, column) for column in required_columns]
        query = select(*columns_to_select)

        # Apply filters
        if ens_id:
            query = query.where(table_class.c.ens_id == str(ens_id)).distinct()
        if session_id:
            query = query.where(table_class.c.session_id == str(session_id))
        query = query.order_by(table_class.c.update_time.desc(), table_class.c.id.desc())
        # Execute query to check if session_id or ens_id exists
        exists_query = select(func.count()).select_from(table_class)

        if ens_id:
            exists_query = exists_query.where(table_class.c.ens_id == str(ens_id))
        if session_id:
            exists_query = exists_query.where(table_class.c.session_id == str(session_id))

        exists_result = await session.execute(exists_query)
        record_count = exists_result.scalar()

        if record_count == 0:
            if session_id and ens_id :
                raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="No data found for the given session_id or ens_id."
                    ) 
            elif session_id :
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No records found for session_id : {session_id}"
                ) 

        # Apply validation status filter
        if extra_filters:
            final_validation_status = extra_filters.get("final_validation_status", "").strip().lower()
            if final_validation_status:
                if final_validation_status == 'review':
                    query = query.where(table_class.c.final_validation_status == FinalValidatedStatus.REVIEW)
                elif final_validation_status == 'auto_reject':
                    query = query.where(table_class.c.final_validation_status == FinalValidatedStatus.AUTO_REJECT)
                elif final_validation_status == 'auto_accept':
                    query = query.where(table_class.c.final_validation_status == FinalValidatedStatus.AUTO_ACCEPT)
                                
            # add additional filter[optional] where screening_ana_status != 'NOT_STARTED'
            screening_analysis_status = extra_filters.get("screening_analysis_status", "").strip().lower()
            if screening_analysis_status:
                if screening_analysis_status == 'active':
                    query = query.where(table_class.c.screening_analysis_status != STATUS.NOT_STARTED)
                elif screening_analysis_status == 'not_started':
                    query = query.where(table_class.c.screening_analysis_status == STATUS.NOT_STARTED)

            # Validate pagination inputs
            offset = extra_filters.get("offset", 0)
            limit = extra_filters.get("limit", 10000)

            if not isinstance(offset, int) or offset < 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'offset' must be a non-negative integer."
                )
            if not isinstance(limit, int) or limit <= 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="'limit' must be a positive integer."
                )

            # Count total rows before pagination
            total_count_query = select(func.count()).select_from(table_class)
            if query._where_criteria:
                total_count_query = total_count_query.filter(*query._where_criteria)

            total_count_result = await session.execute(total_count_query)
            total_count = total_count_result.scalar()

            # Apply offset and limit
            query = query.offset(offset).limit(limit)

            # print("_______query____", query, "\n offset", offset, "\n limit", limit)
        # Execute query
        result = await session.execute(query)
        columns = result.keys()
        rows = result.all()

        formatted_res = [dict(zip(columns, row)) for row in rows]
        try:
            total_count
        except:
            total_count = len(formatted_res)

        logger.debug(f"formatted_res______ {formatted_res}")
        return formatted_res, total_count

    except HTTPException as http_err:
        raise http_err  # Pass FastAPI exceptions as they are

    except SQLAlchemyError as sa_err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(sa_err)}"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )

async def update_dynamic_ens_data(
    table_name: str,
    kpi_data: dict,
    ens_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    """
    Update the specified table dynamically with the provided kpi_data based on ens_id.

    :param session: AsyncSession = Depends(deps.get_session) - The database session.
    :param table_name: str - The name of the table to update.
    :param kpi_data: dict - The dictionary of KPI data to update.
    :param ens_id: str - The ID to filter the record that needs to be updated.
    :return: dict - The result of the update operation.
    """
    try:
        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")
        
        # Prepare the update values
        update_values = {key: value for key, value in kpi_data.items() if value is not None}
        
        # Build the update query
        query = update(table_class).where(table_class.c.ens_id == str(ens_id)).values(update_values)
        
        # Execute the query
        result = await session.execute(query)
        
        # Commit the transaction
        await session.commit()
        
        # Return success response
        return {"status": "success", "message": "Data updated successfully."}

    except ValueError as ve:
        # Handle the case where the table does not exist
        logger.error(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}

async def insert_dynamic_ens_data(
    table_name: str,
    kpi_data: list,
    ens_id: str,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        # Get the table class dynamically
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")
        
        # Add `ens_id` and `session_id` to each row in `kpi_data`
        rows_to_insert = [
            {**row, "ens_id": ens_id, "session_id": session_id}
            for row in kpi_data
        ]
        
        # Build the insert query
        query = insert(table_class).values(rows_to_insert)
        
        # Execute the query
        await session.execute(query)
        
        # Commit the transaction
        await session.commit()
        
        # Return success response
        return {"status": "success", "message": f"Inserted {len(rows_to_insert)} rows successfully."}

    except ValueError as ve:
        # Handle the case where the table does not exist
        logger.error(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}
    
async def insert_dynamic_data(
    table_name: str,
    data: list,
    session: AsyncSession = Depends(deps.get_session)
):
    """
    Insert data dynamically into the specified table without additional constraints.
    
    Args:
        table_name (str): Name of the table where data will be inserted.
        kpi_data (list): List of dictionaries containing the data to insert.
        session (AsyncSession): Async database session.
    
    Returns:
        dict: A dictionary with the status and message of the operation.
    """
    try:
        # Get the table class dynamically from metadata
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(f"Table '{table_name}' does not exist in the database schema.")
        # Get valid column names from the table schema
        valid_columns = set(table_class.columns.keys())

        # Filter data: Keep only valid columns (ignore any extra columns)
        cleaned_data = [
            {key: value for key, value in row.items() if key in valid_columns}
            for row in data
        ]

        # If no valid data remains after filtering, return an error
        if not cleaned_data:
            return {"status": "failure", "message": "No valid data left after filtering extra columns."}

        # Insert filtered data into the table
        query = insert(table_class).values(cleaned_data)
        logger.debug(f"query:  {query}")
        # Execute the insert query
        result = await session.execute(query)  # `result` stores the execution details
        logger.debug(f"rowcount:  {result.rowcount}")
        # Commit the transaction
        await session.commit()

        # Get the number of rows inserted
        rows_inserted = result.rowcount
        logger.info(f"{rows_inserted} row(s) were inserted into the {table_name} table.")
        
        # Return success response
        return {"status": "success", "message": f"Inserted {rows_inserted} rows successfully.", "rows_inserted": rows_inserted}

    except ValueError as ve:
        # Handle cases where the table does not exist
        logger.error(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any unexpected errors
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}
    
async def upsert_session_screening_status(
    columns_data: list,
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        # Get the table class dynamically
        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError(f"Table 'session_screening_status' does not exist in the database schema.")

        # Deduplicate the rows based on session_id
        unique_records = {}
        for record in columns_data:
            record["session_id"] = session_id
            # Use session_id as the key to deduplicate rows
            unique_records[record["session_id"]] = record

        # Convert the dictionary back to a list
        deduplicated_columns_data = list(unique_records.values())

        # Extract column names dynamically
        columns = list(deduplicated_columns_data[0].keys())

        # Prepare bulk insert statement using PostgreSQL ON CONFLICT
        stmt = insert(table_class).values(deduplicated_columns_data)

        # Modify ON CONFLICT to use session_id and update the non-unique fields
        stmt = stmt.on_conflict_do_update(
            index_elements=["session_id"],  # Index on session_id, no unique constraint
            set_={col: stmt.excluded[col] for col in columns if col != "session_id"}
        ).returning(table_class)

        # Execute bulk upsert
        result = await session.execute(stmt)
        await session.commit()

        # Fetch the inserted/updated rows
        return {"message": "Upsert completed", "data": result.fetchall()}

    except ValueError as ve:
        # Handle the case where the table does not exist
        logger.error(f"Error: {ve}")
        return {"error": str(ve), "status": "failure"}
    
    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}
    
    except Exception as e:
        # Catch any other exceptions
        logger.error(f"An unexpected error occurred: {e}")
        return {"error": "An unexpected error occurred", "status": "failure"}
async def update_supplier_master_data(session, session_id) -> Dict:
    try:
        # Fetch table metadata dynamically
        upload_supplier_master_table = Base.metadata.tables.get("upload_supplier_master_data")
        supplier_master_table = Base.metadata.tables.get("supplier_master_data")

        if upload_supplier_master_table is None or supplier_master_table is None:
            raise HTTPException(
                status_code=404,
                detail="Table 'upload_supplier_master_data' or 'supplier_master_data' does not exist in the database schema."
            )

        required_columns = [
            "name", "name_international", "address", "postcode", "city", "country",
            "phone_or_fax", "email_or_website", "national_id", "state", "ens_id", 
            "session_id", "bvd_id", "validation_status", "final_status", "uploaded_name", "uploaded_external_vendor_id"
        ]
        columns_to_select = [
            getattr(upload_supplier_master_table.c, column) for column in required_columns
        ]

        query = select(*columns_to_select).where(
            and_(
                upload_supplier_master_table.c.final_status == FinalStatus.ACCEPTED,
                upload_supplier_master_table.c.session_id == session_id,
                upload_supplier_master_table.c.bvd_id.isnot(None)
            )
        )

        result = await session.execute(query)
        columns = result.keys()
        rows = result.fetchall()

        if not rows:
            return {
                "status": "success",
                "session_id": session_id,
                "message": f"No valid records found for session_id: {session_id}. No updates were performed.",
                "updated_ens_ids": []
            }

        RENAME_MAP = {"uploaded_external_vendor_id": "external_vendor_id"}

        rows_to_insert = [
            {RENAME_MAP.get(k, k): v for k, v in zip(columns, row)}
            for row in rows
        ]

        query2 = insert(supplier_master_table).values(rows_to_insert)
        query2 = query2.on_conflict_do_update(
            index_elements=["ens_id", "session_id"],
            set_={col: query2.excluded[col] for col in rows_to_insert[0].keys() if col not in ["ens_id", "session_id"]}
        ).returning(supplier_master_table.c.ens_id)

        result = await session.execute(query2)
        inserted_or_updated_rows = result.fetchall()

        if not inserted_or_updated_rows:
            return {
                "status": "success",
                "session_id": session_id,
                "message": "No changes were made as no new data was available for insertion or update.",
                "updated_ens_ids": []
            }

        # Commit the transaction
        await session.commit()

        # Extract updated ens_ids
        updated_ens_ids = [row[0] for row in inserted_or_updated_rows]

        return {
            "status": "success",
            "session_id": session_id,
            "message": f"Inserted or updated {len(updated_ens_ids)} rows successfully.",
            "updated_ens_ids": updated_ens_ids
        }

    except HTTPException as http_err:
        raise http_err

    except SQLAlchemyError as db_err:
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(db_err)}"
        )

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(error)}"
        )
async def validate_user_request(current_user, session: AsyncSession = Depends(deps.get_session)):
    # Get the tables from metadata
    supplier_screening_table = Base.metadata.tables.get("session_screening_status")
    upload_supplier_data = Base.metadata.tables.get("upload_supplier_master_data")
    user_table = Base.metadata.tables.get("users_table")
    if supplier_screening_table is None or upload_supplier_data is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="One or more required tables do not exist in the database schema."
        )

    # Alias for readability (optional)
    s = aliased(supplier_screening_table)
    u = aliased(upload_supplier_data)
    ut = aliased(user_table)
    # Extract user group & user ID correctly
    user_group, user_id = current_user['user_group'], current_user['user_id']

    # Build the async query
    one_hour_ago = datetime.utcnow() - timedelta(hours=1)

    query = select(func.count(func.distinct(tuple_(s.c.session_id, s.c.overall_status, ut.c.user_group)))).select_from(
        s.join(u, s.c.session_id == u.c.session_id).join(ut, u.c.user_id == ut.c.user_id)
    ).where(
        s.c.overall_status == STATUS.IN_PROGRESS.value,
        u.c.user_id == user_id,
        ut.c.user_group == user_group, 
        s.c.create_time >= one_hour_ago
    )
        
    result = await session.execute(query)
    logger.debug(f"result.scalar() {result}")
    count = result.scalar_one_or_none()  # Returns None if no rows found
    logger.debug(f"Query Result: {count}")
    return count

async def run_neo4j_query(cypher_query: str) -> dict:
    try:

        URI = os.environ.get("GRAPHDB__URI")
        USER = os.environ.get("GRAPHDB__USER")
        PASSWORD = os.environ.get("GRAPHDB__PASSWORD")

        async with AsyncGraphDatabase.driver(URI, auth=(USER, PASSWORD)) as driver:
            async with driver.session() as session:
                result = await session.run(cypher_query)

                # Try fetching records (for read queries)
                try:
                    records = await result.data()
                    return {
                        "status": "pass",
                        "message": "Query executed successfully.",
                        "records": records
                    }
                except Exception:
                    # For write queries that don't return anything
                    return {
                        "status": "pass",
                        "message": "Query executed successfully. No return values."
                    }

    except Exception as e:
        return {
            "status": "fail",
            "message": f"Error executing query: {str(e)}"
        }

async def default_head_graph(client_id, session):
    """
    Creates a root 'Aramco' node with a unique ID in the Neo4j graph.
    """

    cypher_query = f'''
    CREATE (a:Company {{name: "Aramco", id: "{client_id}"}})
    '''

    # Run the Cypher query
    result = await run_neo4j_query(cypher_query)

    return {
        "status": "pass",
        "message": "Aramco node created successfully.",
        "client_id": client_id,
        "neo4j_result": result,
    }


async def upsert_session_config(client_id_, session_id_, session) -> Dict:
    try:
        # Get the table class dynamically
        table_class = Base.metadata.tables.get("client_configuration")
        if table_class is None:
            raise ValueError(f"Table 'client_configuration' does not exist in the database schema.")

        # Prepare columns to select
        query = select(table_class).where((table_class.c.client_id == str(client_id_)) &
            (table_class.c.module_enabled_status == True))


        # Execute
        result = await session.execute(query)
        columns = result.keys()
        rows = result.all()

        formatted_res = [dict(zip(columns, row)) for row in rows]
        upserted_records = []
        logger.debug(f"formatted_res {formatted_res}")
        if formatted_res and len(formatted_res):
            table_class = Base.metadata.tables.get("session_configuration")
            if table_class is None:
                raise ValueError(f"Table 'session_configuration' does not exist in the database schema.")

            for i in range(len(formatted_res)):
                logger.debug(f"formatted_res[i]['module_enabled_status'] {formatted_res[i]['module_enabled_status']}")
                if formatted_res[i]['module_enabled_status']:
                    new_row = {
                        'client_id': client_id_,
                        'session_id': session_id_,
                        'module' : formatted_res[i]['kpi_theme'],
                        'module_active_status' : bool(formatted_res[i]['module_enabled_status'])
                    }
                    stmt = insert(table_class).values(**new_row)

                    stmt = stmt.on_conflict_do_update(
                        index_elements=[
                            "session_id", "module"
                        ],
                        set_={
                            'module_active_status': stmt.excluded.module_active_status
                        }
                    )
                    
                    logger.debug(f"stmt {stmt}")
                    # Execute
                    await session.execute(stmt)

                    # Add to upserted record
                    upserted_records.append(new_row)

            # Commit once after all upserts
            await session.commit()

        return {"message": "Upsert completed", "data": upserted_records}
    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid file format: {str(ve)}"
        )

    except HTTPException as http_err:
        raise http_err  # Re-raise FastAPI HTTP exceptions

    except SQLAlchemyError as sa_err:
        # Handle SQLAlchemy-specific errors
        logger.error(f"Database error: {sa_err}")
        return {"error": "Database error", "status": "failure"}

    except Exception as error:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing session config: {str(error)}"
        )


async def get_latest_session_for_ens_id(
        table_name: str,
        required_columns: list,
        ens_id: str = "",
        session=None,
):
    try:
        session_id = False

        # Validate if table exists
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table '{table_name}' does not exist in the database schema."
            )

        # Prepare columns to select
        required_columns += ["update_time", "id"]
        columns_to_select = [getattr(table_class.c, column) for column in required_columns]
        query = select(*columns_to_select)

        # Apply filters
        if ens_id:
            query = query.where(table_class.c.ens_id == str(ens_id) and table_class.c.overall_status == "COMPLETED").distinct()
        if session_id:
            query = query.where(table_class.c.session_id == str(session_id))

        query = query.order_by(table_class.c.update_time.desc(), table_class.c.id.desc())
        query = query.limit(1)


        # Execute query to check if session_id or ens_id exists
        exists_query = select(func.count()).select_from(table_class)

        if ens_id:
            exists_query = exists_query.where(table_class.c.ens_id == str(ens_id) and table_class.c.overall_status == "COMPLETED")
        if session_id:
            exists_query = exists_query.where(table_class.c.session_id == str(session_id))

        exists_result = await session.execute(exists_query)
        record_count = exists_result.scalar()

        if record_count == 0:
            if session_id and ens_id :
                raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="No data found for the given session_id or ens_id."
                    ) 
            elif session_id :
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No records found for session_id : {session_id}"
                ) 

        # Execute query
        result = await session.execute(query)
        columns = result.keys()
        rows = result.all()

        if rows:
            formatted_res = [dict(zip(columns, rows[0]))]  # Get the first (top) row
        else:
            formatted_res = []  # Return empty if no rows are returned

        return formatted_res

    except HTTPException as http_err:
        raise http_err  # Pass FastAPI exceptions as they are

    except SQLAlchemyError as sa_err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(sa_err)}"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )

async def get_dynamic_ens_data_for_session(
        table_name: str,
        required_columns: list,
        ens_id: str,
        session_id: str,
        session: AsyncSession = Depends(deps.get_session)
):
    try:
        # session = SessionFactory()
        table_class = Base.metadata.tables.get(table_name)
        if table_class is None:
            raise ValueError(
                f"Table '{table_name}' does not exist in the database schema."
            )

        # If "*" is passed, select all columns
        if required_columns == ["all"]:
            columns_to_select = [table_class.c[column] for column in table_class.c.keys()]
        else:
            columns_to_select = [getattr(table_class.c, column) for column in required_columns]

        query = select(*columns_to_select)

        if ens_id:
            query = query.where(table_class.c.ens_id == str(ens_id)).distinct()

        if session_id:
            query = query.where(table_class.c.session_id == str(session_id))

        result = await session.execute(query)

        columns = result.keys()
        rows = result.all()

        formatted_res = [
            dict(zip(columns, row)) for row in rows
        ]

        await session.close()
        return formatted_res

    except ValueError as ve:
        print(f"Error: {ve}")
        return []

    except SQLAlchemyError as sa_err:
        print(f"Database error: {sa_err}")
        return []

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

async def get_session_screening_status_static(
    session_id: str,
    session: AsyncSession = Depends(deps.get_session)
):
    try:
        required_columns = [
            "id", "session_id", "overall_status", "list_upload_status",
            "supplier_name_validation_status", "screening_analysis_status",
            "create_time", "update_time"
        ]

        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError("Table 'session_screening_status' does not exist in the schema.")

        query = select(*(getattr(table_class.c, col) for col in required_columns))
        query = query.where(table_class.c.session_id == session_id)

        result = await session.execute(query)
        rows = result.fetchall()

        if not rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No records found for session_id: {session_id}"
            )

        return [dict(zip(result.keys(), row)) for row in rows]

    except ValueError as ve:
        logger.error(f"Schema error: {ve}")
        return []

    except SQLAlchemyError as sa_err:
        logger.error(f"Database error: {sa_err}")
        return []

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return []