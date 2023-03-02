export interface PageCategory {
  /**
   * Category name
   *
   * 分类名称
   */
  name: string;

  /**
   * Category path
   *
   * 分类路径
   */
  path?: string;
}

export type PageTag = PageCategory;
