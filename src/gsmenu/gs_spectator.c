#include <lvgl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ui.h"
#include "../input.h"
#include "helper.h"
#include "styles.h"

#include "../stream_rebroadcast.h"

extern lv_obj_t * menu;
extern lv_group_t * default_group;
extern StreamRebroadcast *rebroadcaster;

static lv_obj_t * spectator_switch = NULL;

static void spectator_toggle_event_handler(lv_event_t * e) {
    lv_event_code_t code = lv_event_get_code(e);
    if (code == LV_EVENT_VALUE_CHANGED) {
        lv_obj_t * sw = lv_event_get_target(e);
        int checked = lv_obj_has_state(sw, LV_STATE_CHECKED);
        if (rebroadcaster != NULL) {
            if (checked) {
                rebroadcast_start(rebroadcaster);
            } else {
                rebroadcast_stop(rebroadcaster);
            }
        }
    }
}

void spectator_page_load_callback(lv_obj_t * page) {
    menu_page_data_t * menu_page_data = (menu_page_data_t *) lv_obj_get_user_data(page);
    lv_indev_set_group(lv_indev_get_next(NULL), menu_page_data->indev_group);
    lv_group_set_default(menu_page_data->indev_group);

    if (spectator_switch != NULL) {
        lv_obj_t * sw = lv_obj_get_child_by_type(spectator_switch, 0, &lv_switch_class);
        if (sw) {
            if (rebroadcast_is_enabled()) {
                lv_obj_add_state(sw, LV_STATE_CHECKED);
            } else {
                lv_obj_remove_state(sw, LV_STATE_CHECKED);
            }
        }
    }

    lv_group_set_default(default_group);
}

void create_gs_spectator_menu(lv_obj_t * parent) {

    menu_page_data_t* menu_page_data = malloc(sizeof(menu_page_data_t));
    strcpy(menu_page_data->type, "gs");
    strcpy(menu_page_data->page, "spectator");
    menu_page_data->page_load_callback = spectator_page_load_callback;
    menu_page_data->indev_group = lv_group_create();
    menu_page_data->entry_count = 0;
    menu_page_data->page_entries = NULL;
    lv_group_set_default(menu_page_data->indev_group);
    lv_obj_set_user_data(parent, menu_page_data);

    lv_obj_t * section = lv_menu_section_create(parent);
    lv_obj_add_style(section, &style_openipc_section, 0);

    lv_obj_t * cont = lv_menu_cont_create(section);
    lv_obj_set_flex_flow(cont, LV_FLEX_FLOW_COLUMN);

    /* Toggle switch */
    spectator_switch = create_switch(cont, LV_SYMBOL_WIFI, "Spectator Stream", NULL, menu_page_data, false);
    lv_obj_t * sw = lv_obj_get_child_by_type(spectator_switch, 0, &lv_switch_class);
    if (sw) {
        lv_obj_add_event_cb(sw, spectator_toggle_event_handler, LV_EVENT_VALUE_CHANGED, NULL);
        if (rebroadcast_is_enabled()) {
            lv_obj_add_state(sw, LV_STATE_CHECKED);
        }
    }

    /* Info label */
    lv_obj_t * info_cont = lv_menu_cont_create(section);
    lv_obj_set_flex_flow(info_cont, LV_FLEX_FLOW_COLUMN);
    lv_obj_t * info_label = lv_label_create(info_cont);
    lv_label_set_text(info_label,
        "Rebroadcasts the FPV stream over\n"
        "WiFi (UDP/RTP) for a spectator\n"
        "view on a phone.\n\n"
        "On Android, use a player like\n"
        "VLC or FPV_VR_OS and connect to\n"
        "the RTP stream on port 5700.");
    lv_obj_add_style(info_label, &style_openipc_section, LV_PART_MAIN);
    lv_obj_set_width(info_label, LV_PCT(100));

    lv_group_set_default(default_group);
}
