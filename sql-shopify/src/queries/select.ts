export const selectCount = (table: string): string => {
  return `SELECT COUNT(*) AS c FROM ${table}`;
};

export const selectRowById = (id: number, table: string): string => {
  return `SELECT * FROM apps WHERE id=${id}`;
};

export const selectCategoryByTitle = (title: string): string => {
  return `SELECT * FROM categories WHERE title='${title}'` ;
};

export const selectAppCategoriesByAppId = (appId: number): string => {
  return `SELECT apps.title AS app_title,categories.id AS category_id,categories.title AS category_title
            FROM apps
            JOIN apps_categories ON apps_categories.app_id=apps.id
            JOIN categories ON categories.id=apps_categories.category_id
            WHERE apps.id=${appId}`;
};

export const selectUnigueRowCount = (tableName: string, columnName: string): string => {
  return `SELECT COUNT(DISTINCT ${columnName}) AS c FROM ${tableName}`;
};

export const selectReviewByAppIdAuthor = (appId: number, author: string): string => {
  return `SELECT * FROM reviews WHERE app_id=${appId} AND author='${author}'`;
};

export const selectColumnFromTable = (columnName: string, tableName: string): string => {
  return `SELECT ${columnName} FROM ${tableName}`;
};

