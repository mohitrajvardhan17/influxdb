import React, {Component} from 'react'

// Components
import {Page} from '@influxdata/clockface'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'

class SettingsHeader extends Component {
  public render() {
    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>
          <PageTitleWithOrg title="Settings" />
        </Page.Header.Left>
        <Page.Header.Right />
      </Page.Header>
    )
  }
}

export default SettingsHeader
