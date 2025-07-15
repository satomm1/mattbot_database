import sqlite3
import os
from datetime import datetime

class RobotDatabase:
    """
    A class to manage SQLite database operations for robot data.
    
    This class provides methods to:
    - Initialize/connect to the database
    - Create tables for goals and detected objects
    - Add and retrieve robot goals
    - Add and retrieve detected objects
    """

    def __init__(self, db_path='/workspace/catkin_ws/src/mattbot_database/robot_data.db'):
        """
        Initialize the database connection.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path

        if not os.path.exists(self.db_path):
            print(f"Creating new database: {self.db_path}")

        self.conn = None
        self.cursor = None

    def connect(self):
        """
        Connect to the database and initialize a cursor.
        """
        # Create the database file if it doesn't exist
        if not os.path.exists(self.db_path):
            print(f"Creating new database: {self.db_path}")
        
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
    
    def close(self):
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
    
    # Table creation methods
    
    def create_goals_table(self):
        """
        Create the goals table if it doesn't exist.
        """
        self.connect()
        try:
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS goals (
                goal_id INTEGER PRIMARY KEY,
                robot_id INTEGER NOT NULL,
                x REAL NOT NULL,
                y REAL NOT NULL,
                theta REAL NOT NULL,
                timestamp INTEGER DEFAULT (strftime('%s','now'))
            )
            """)
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error creating goals table: {e}")
            return False
        finally:
            self.close()
    
    def create_objects_table(self):
        """
        Create the objects table if it doesn't exist.
        """
        self.connect()
        try:
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS objects (
                object_id INTEGER PRIMARY KEY,
                class_name TEXT NOT NULL,
                x REAL NOT NULL,
                y REAL NOT NULL,
                robot_id INTEGER,
                timestamp INTEGER DEFAULT (strftime('%s','now'))
            )
            """)
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error creating objects table: {e}")
            return False
        finally:
            self.close()
    
    # Goal-related methods
    
    def add_goal(self, robot_id, x, y, theta, timestamp):
        """
        Add a new goal for a robot.
        
        Args:
            robot_id: ID of the robot
            x: X-coordinate of the goal
            y: Y-coordinate of the goal
            theta: Orientation angle of the goal in radians
            timestamp: Timestamp of the goal in integer format (e.g., Unix timestamp)
        
        Returns:
            goal_id: ID of the newly added goal, or None if there was an error
        """
        self.connect()
        try:
            self.cursor.execute(
                "INSERT INTO goals (robot_id, x, y, theta, timestamp) VALUES (?, ?, ?, ?, ?)",
                (robot_id, x, y, theta, timestamp)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error adding goal: {e}")
            return None
        finally:
            self.close()
    
    def get_latest_goal(self, robot_id):
        """
        Get the latest goal for a specific robot.
        
        Args:
            robot_id: ID of the robot
        
        Returns:
            A dictionary containing the goal data, or None if no goal was found
        """
        self.connect()
        try:
            self.cursor.execute(
                "SELECT goal_id, robot_id, x, y, theta, timestamp FROM goals "
                "WHERE robot_id = ? ORDER BY timestamp DESC LIMIT 1",
                (robot_id,)
            )
            goal = self.cursor.fetchone()
            
            if goal:
                return {
                    'goal_id': goal[0],
                    'robot_id': goal[1],
                    'x': goal[2],
                    'y': goal[3],
                    'theta': goal[4],
                    'timestamp': goal[5]
                }
            return None
        except sqlite3.Error as e:
            print(f"Error getting latest goal: {e}")
            return None
        finally:
            self.close()
    
    def get_goal_history(self, robot_id, limit=10):
        """
        Get the history of goals for a specific robot.
        
        Args:
            robot_id: ID of the robot
            limit: Maximum number of goals to retrieve
        
        Returns:
            A list of dictionaries containing the goal data
        """
        self.connect()
        try:
            self.cursor.execute(
                "SELECT goal_id, robot_id, x, y, theta, timestamp FROM goals "
                "WHERE robot_id = ? ORDER BY timestamp DESC LIMIT ?",
                (robot_id, limit)
            )
            goals = self.cursor.fetchall()
            
            result = []
            for goal in goals:
                result.append({
                    'goal_id': goal[0],
                    'robot_id': goal[1],
                    'x': goal[2],
                    'y': goal[3],
                    'theta': goal[4],
                    'timestamp': goal[5]
                })
            return result
        except sqlite3.Error as e:
            print(f"Error getting goal history: {e}")
            return []
        finally:
            self.close()
    
    # Object-related methods
    
    def add_object(self, class_name, x, y, robot_id, timestamp):
        """
        Add a detected object to the database.
        
        Args:
            class_name: The class/type of the detected object
            x: X-coordinate of the object
            y: Y-coordinate of the object
            robot_id: ID of the robot that detected the object
            timestamp: Timestamp of the detection

        Returns:
            object_id: ID of the newly added object, or None if there was an error
        """
        self.connect()
        try:    
            self.cursor.execute(
                "INSERT INTO objects (class_name, x, y, robot_id, timestamp) VALUES (?, ?, ?, ?, ?)",
                (class_name, x, y, robot_id, timestamp)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error adding object: {e}")
            return None
        finally:
            self.close()
    
    def get_recent_objects(self, limit=10, class_name=None, robot_id=None):
        """
        Get recently detected objects.
        
        Args:
            limit: Maximum number of objects to retrieve
            class_name: Filter by class name (optional)
            robot_id: Filter by robot ID (optional)
        
        Returns:
            A list of dictionaries containing the object data
        """
        self.connect()
        try:
            query = "SELECT object_id, class_name, x, y, robot_id, timestamp FROM objects"
            params = []
            
            conditions = []
            if class_name:
                conditions.append("class_name = ?")
                params.append(class_name)
            
            if robot_id is not None:
                conditions.append("robot_id = ?")
                params.append(robot_id)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY timestamp DESC LIMIT ?"
            params.append(limit)
            
            self.cursor.execute(query, params)
            objects = self.cursor.fetchall()
            
            result = []
            for obj in objects:
                result.append({
                    'object_id': obj[0],
                    'class_name': obj[1],
                    'x': obj[2],
                    'y': obj[3],
                    'robot_id': obj[4],
                    'timestamp': obj[5]
                })
            return result
        except sqlite3.Error as e:
            print(f"Error getting recent objects: {e}")
            return []
        finally:
            self.close()

# Helper functions for direct use without instantiating the class

def initialize_database(db_path='robot_data.db'):
    """
    Initialize the database and create required tables.
    
    Args:
        db_path: Path to the SQLite database file
    
    Returns:
        True if successful, False otherwise
    """
    try:
        with RobotDatabase(db_path) as db:
            db.create_goals_table()
            db.create_objects_table()
        return True
    except Exception as e:
        print(f"Error initializing database: {e}")
        return False

def add_robot_goal(robot_id, x, y, theta, db_path='robot_data.db'):
    """
    Add a new goal for a robot.
    
    Args:
        robot_id: ID of the robot
        x: X-coordinate of the goal
        y: Y-coordinate of the goal
        theta: Orientation angle of the goal in radians
        db_path: Path to the SQLite database file
    
    Returns:
        goal_id: ID of the newly added goal, or None if there was an error
    """
    try:
        with RobotDatabase(db_path) as db:
            return db.add_goal(robot_id, x, y, theta)
    except Exception as e:
        print(f"Error adding robot goal: {e}")
        return None

def get_robot_goal(robot_id, db_path='robot_data.db'):
    """
    Get the latest goal for a specific robot.
    
    Args:
        robot_id: ID of the robot
        db_path: Path to the SQLite database file
    
    Returns:
        A dictionary containing the goal data, or None if no goal was found
    """
    try:
        with RobotDatabase(db_path) as db:
            return db.get_latest_goal(robot_id)
    except Exception as e:
        print(f"Error getting robot goal: {e}")
        return None

def get_robot_goal_history(robot_id, limit=10, db_path='robot_data.db'):
    """
    Get the history of goals for a specific robot.
    
    Args:
        robot_id: ID of the robot
        limit: Maximum number of goals to retrieve
        db_path: Path to the SQLite database file
    
    Returns:
        A list of dictionaries containing the goal data
    """
    try:
        with RobotDatabase(db_path) as db:
            return db.get_goal_history(robot_id, limit)
    except Exception as e:
        print(f"Error getting robot goal history: {e}")
        return []

def add_detected_object(class_name, x, y, theta, robot_id=None, db_path='robot_data.db'):
    """
    Add a detected object to the database.
    
    Args:
        class_name: The class/type of the detected object
        x: X-coordinate of the object
        y: Y-coordinate of the object
        theta: Orientation angle of the object in radians
        robot_id: ID of the robot that detected the object (optional)
        db_path: Path to the SQLite database file
    
    Returns:
        object_id: ID of the newly added object, or None if there was an error
    """
    try:
        with RobotDatabase(db_path) as db:
            return db.add_object(class_name, x, y, theta, robot_id)
    except Exception as e:
        print(f"Error adding detected object: {e}")
        return None

def get_recent_detected_objects(limit=10, class_name=None, robot_id=None, db_path='robot_data.db'):
    """
    Get recently detected objects.
    
    Args:
        limit: Maximum number of objects to retrieve
        class_name: Filter by class name (optional)
        robot_id: Filter by robot ID (optional)
        db_path: Path to the SQLite database file
    
    Returns:
        A list of dictionaries containing the object data
    """
    try:
        with RobotDatabase(db_path) as db:
            return db.get_recent_objects(limit, class_name, robot_id)
    except Exception as e:
        print(f"Error getting recent objects: {e}")
        return []