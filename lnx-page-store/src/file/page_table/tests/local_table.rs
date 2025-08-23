use crate::file::page_table::local::LocalPageTable;
use crate::layout::page_metadata::PageMetadata;
use crate::{PageFileId, PageGroupId, PageId};

#[rstest::rstest]
fn test_modify_local_page_table() {
    let table = LocalPageTable::default();

    let page = table.get_page(PageId(0));
    assert_eq!(page, None);
    let page = table.get_page(PageId(63));
    assert_eq!(page, None);
    let page = table.get_page(PageId(64));
    assert_eq!(page, None);
    let page = table.get_page(PageId(500));
    assert_eq!(page, None);

    let pages = [(0, 5), (4, 7), (3, 5), (63, 2), (64, 1), (499, 12)];

    for (page_id, group_id) in pages {
        let mut page = PageMetadata::empty();
        page.group = PageGroupId(group_id);
        page.id = PageId(page_id);
        table.insert_page(page);
    }

    let page = table.get_page(PageId(0));
    assert_eq!(
        page,
        Some(PageMetadata {
            group: PageGroupId(5),
            next_page_file_id: PageFileId(0),
            next_page_id: PageId::TERMINATOR,
            id: PageId(0),
            data_len: 0,
            context: [0; 40],
        }),
    );
    let page = table.get_page(PageId(2));
    assert_eq!(page, None);
    let page = table.get_page(PageId(63));
    assert_eq!(
        page,
        Some(PageMetadata {
            group: PageGroupId(2),
            next_page_file_id: PageFileId(0),
            next_page_id: PageId::TERMINATOR,
            id: PageId(63),
            data_len: 0,
            context: [0; 40],
        }),
    );
    let page = table.get_page(PageId(64));
    assert_eq!(
        page,
        Some(PageMetadata {
            group: PageGroupId(1),
            next_page_file_id: PageFileId(0),
            next_page_id: PageId::TERMINATOR,
            id: PageId(64),
            data_len: 0,
            context: [0; 40],
        }),
    );
    let page = table.get_page(PageId(500));
    assert_eq!(page, None);
}
