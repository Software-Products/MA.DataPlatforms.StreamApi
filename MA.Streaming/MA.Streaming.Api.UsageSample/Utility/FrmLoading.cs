// <copyright file="FrmLoading.cs" company="McLaren Applied Ltd.">
//
// Copyright 2024 McLaren Applied Ltd
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

using System.Timers;

using Timer = System.Timers.Timer;

namespace MA.Streaming.Api.UsageSample.Utility;

public partial class FrmLoading : Form
{
    private readonly Timer timer = new();

    public FrmLoading()
    {
        this.InitializeComponent();
        this.timer.Elapsed += this.Timer_Elapsed;
    }

    private void Timer_Elapsed(object? sender, ElapsedEventArgs e)
    {
        this.Invoke((MethodInvoker)this.Close);
        this.timer.Enabled = false;
    }

    public void ShowOnForm(Form parentForm, TimeSpan? showTime = null)
    {
        this.StartPosition = FormStartPosition.Manual;
        this.DesktopLocation = parentForm.DesktopLocation;
        this.Size = parentForm.Size;
        if (showTime != null)
        {
            this.timer.Interval = showTime.Value.TotalMilliseconds;
            this.timer.Enabled = true;
        }

        this.ShowDialog();
    }
}
