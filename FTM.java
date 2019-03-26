/*
File Description: This program executes SQL commands on a database after 
proper authentification. Client-side operations with data from SQL server
should be minimized i.e. the client should act as primarily a controller.
*/

import java.sql.*; 

// Compute transitive closure on PA2Bank customers to find depositors
// that are connected (reachable) to other depositors s.t. a connection is
// depositor d1 transfers a fund from d1.ano to d2.ano of depositor d2 
public class FTM {

   public static void main(String[] args) {
      // Create query for funds(c1, c2) view
      String cr_funds = "CREATE TABLE funds (c1 varchar(40) NOT NULL, c2 varchar(40) NOT NULL);";
      String in_funds = "INSERT INTO funds (SELECT d1.cname AS name1, d2.cname AS name2 FROM depositor d1, depositor d2, transfer t WHERE d1.ano=t.src AND d2.ano=tgt);";
      String dr_funds = "DROP TABLE funds;";
   
      // Create query for T table which contains all funds(c1, c2)
      String cr_T = "CREATE TABLE T (name1 varchar(40) NOT NULL, name2 varchar(40) NOT NULL);";
      String in_T = "INSERT INTO T (SELECT * FROM funds);"; 
      String ud_T0 = "INSERT INTO T (SELECT * FROM T_U);";
      String ud_T1 = "INSERT INTO T (SELECT * FROM T_old WHERE (name1, name2) NOT IN (SELECT * FROM T_U));";
      String dl_T = "DELETE FROM T;";
      String dr_T = "DROP TABLE T;";
      
      // Create query for T_old table which contains all tuples of T
      String cr_Told = "CREATE TABLE T_old (name1 varchar(40) NOT NULL, name2 varchar(40) NOT NULL);";
      String in_Told = "INSERT INTO T_old (SELECT * FROM T);";
      String dl_Told = "DELETE FROM T_old;"; 
      String dr_Told = "DROP TABLE T_old;";
      
      // Create query for D table which contains all funds(c1, c2)
      String cr_D = "CREATE TABLE D (name1 varchar(40) NOT NULL, name2 varchar(40) NOT NULL);";
      String in_D = "INSERT INTO D (SELECT * FROM funds);";
      String ud_D = "INSERT INTO D (SELECT * FROM T WHERE (name1, name2) NOT IN (SELECT * FROM T_old))"; 
      String sel_all_Ds = "SELECT * FROM D;";
      String dl_D = "DELETE FROM D;"; 
      String dr_D = "DROP TABLE D;";

      // Create recursive query for finding connectivity between any <u,v> in G(|V|)
      String cr_TU = "CREATE TABLE T_U (name1 varchar(40) NOT NULL, name2 varchar(40) NOT NULL);";
      String in_TU = "INSERT INTO T_U ((SELECT * FROM T) UNION (SELECT x.name1, y.name2 FROM D x, T y WHERE x.name2=y.name1) UNION (SELECT x.name1, y.name2 FROM T x, D y WHERE x.name2=y.name1));";
      String dl_TU = "DELETE FROM T_U;";
      String dr_TU = "DROP TABLE T_U;";

      // Create query for influence view which contains pairs <u, v>
      // such that customer u is reachable to customer v
      String cr_inf = "CREATE TABLE influence (\"from\" varchar(40) NOT NULL, \"to\" varchar(40) NOT NULL);";
      String in_inf = "INSERT INTO influence (SELECT name1, name2 FROM T);";
      String sel_all_inf = "SELECT * FROM influence;"; 
      String dr_inf = "DROP TABLE IF EXISTS influence;";

      String db_serv = "jdbc:postgresql://localhost:5432/PA2Bank";
      String u_name = args[0];
      String pw = args[1];
      
      try (
         Connection c = DriverManager.getConnection(db_serv, u_name, pw);
         Statement stmt = c.createStatement();
      ) {
         stmt.execute(dr_inf); 
         stmt.execute(cr_funds);         
         stmt.execute(in_funds);
         stmt.execute(cr_T);
         stmt.execute(cr_Told);
         stmt.execute(cr_TU);
         stmt.execute(cr_D);

         stmt.execute(in_T);
         stmt.execute(in_D);

         ResultSet D_res = stmt.executeQuery(sel_all_Ds);
         while (D_res.next()) {
            stmt.execute(in_Told);
            stmt.execute(in_TU);
            stmt.execute(dl_T);
            stmt.execute(ud_T0);
            stmt.execute(ud_T1);

            stmt.execute(dl_D);
            stmt.execute(ud_D);
            stmt.execute(dl_Told);
            stmt.execute(dl_TU);
            D_res = stmt.executeQuery(sel_all_Ds);
         }

         stmt.execute(cr_inf);
         stmt.execute(in_inf);
         
         ResultSet inf = stmt.executeQuery(sel_all_inf);
         System.out.println("HERE ARE THE RESULT OF influence(from, to):");
         
         while (inf.next()) {
            String n1 = inf.getString("from");
            String n2 = inf.getString("to");
            System.out.println("("+n1+", "+n2+")");
         }

         // Delete auxilary tables
         stmt.execute(dr_TU);
         stmt.execute(dr_Told);
         stmt.execute(dr_T);
         stmt.execute(dr_D);
         stmt.execute(dr_funds);

      } catch(SQLException ex) { ex.printStackTrace(); } 
   }
}
